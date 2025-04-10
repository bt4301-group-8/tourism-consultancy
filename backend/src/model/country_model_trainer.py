import os
import glob
import json
import joblib
import pandas as pd
import numpy as np
import xgboost as xgb
from typing import List, Dict, Tuple, Optional, Any

from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll import scope

from backend.src.services import logger


class CountryModelTrainer:
    """
    trains an xgboost model for predicting visitor numbers for individual countries.

    handles data loading, hyperparameter tuning using hyperopt, final model
    training, and saving artifacts (model, parameters, trials).
    """

    def __init__(
        self,
        data_path: str = "backend/data/processed/countries/",  # temp, before migrating to DB
        output_dir: str = "backend/trained_models/",
        file_pattern: str = "*_final_df.csv",
        target_variable: str = "log_visitors",
        original_target: Optional[str] = "num_visitors",
        cols_to_drop: List[str] = [
            "month_year",
            "country",
            "num_visitors",  # identifiers and original target
            "reddit_sentiment",
            "ig_sentiment",  # dropping non-scaled sentiment columns
            "reddit_sentiment_label",
            "ig_sentiment_label",  # labels (if using encoded versions)
            "reddit_under-performing",
            "reddit_normal",
            "reddit_outperforming",
            "ig_under-performing",
            "ig_normal",
            "ig_outperforming",
            # TODO: add any other columns that should not be features
        ],
        n_cv_splits: int = 5,
        max_hyperopt_evals: int = 50,
        early_stopping_rounds_cv: int = 25,
        early_stopping_rounds_final: int = 50,
        final_model_boost_rounds: int = 1500,  # high number for final training with early stopping
        seed: int = 42,
    ):
        """
        initializes the trainer with configuration settings.

        args:
            data_path: path to the directory containing processed country csv files.
            output_dir: path to the directory where models and results will be saved.
            file_pattern: glob pattern to identify country csv files.
            target_variable: the name of the column to predict (e.g., 'log_visitors').
            original_target: the name of the original, non-transformed target variable (optional).
            cols_to_drop: a list of column names to exclude from features.
            n_cv_splits: number of folds for time series cross-validation during hyperopt.
            max_hyperopt_evals: maximum number of hyperparameter evaluations.
            early_stopping_rounds_cv: early stopping rounds for xgb during hyperopt cv folds.
            early_stopping_rounds_final: early stopping rounds for final model training.
            final_model_boost_rounds: max boost rounds for final model (early stopping determines actual).
            seed: random seed for reproducibility.
        """
        self.data_path = data_path
        self.output_dir = output_dir
        self.file_pattern = file_pattern
        self.target_variable = target_variable
        self.cols_to_drop = list(set(cols_to_drop + [target_variable]))
        self.original_target = original_target
        self.n_cv_splits = n_cv_splits
        self.max_hyperopt_evals = max_hyperopt_evals
        self.early_stopping_rounds_cv = early_stopping_rounds_cv
        self.early_stopping_rounds_final = early_stopping_rounds_final
        self.final_model_boost_rounds = final_model_boost_rounds
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        # define hyperparameter search space
        self.search_space = {
            "objective": "reg:squarederror",
            "eval_metric": "rmse",
            "eta": hp.loguniform("eta", np.log(0.01), np.log(0.3)),
            "max_depth": scope.int(hp.quniform("max_depth", 3, 10, 1)),
            "subsample": hp.uniform("subsample", 0.6, 1.0),
            "colsample_bytree": hp.uniform("colsample_bytree", 0.6, 1.0),
            "min_child_weight": hp.quniform("min_child_weight", 1, 10, 1),
            "gamma": hp.loguniform("gamma", np.log(0.01), np.log(1.0)),
            "lambda": hp.loguniform("lambda", np.log(1.0), np.log(4.0)),
            "alpha": hp.loguniform("alpha", np.log(0.01), np.log(1.0)),
            "seed": self.seed,
        }

        # create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        self.logger = logger
        self.logger.info(f"output directory set to: {self.output_dir}")

    def _load_and_prepare_data(
        self,
        csv_path: str,
    ) -> Optional[Tuple[pd.DataFrame, pd.Series, List[str]]]:
        """
        loads data from a csv, prepares it for modeling (datetime index, sorting, feature selection).

        args:
            csv_path: path to the country's csv file.

        returns:
            a tuple containing (features dataframe, target series, list of feature names),
            or none if loading/preparation fails.
        """
        try:
            df = pd.read_csv(csv_path)
            country_name = os.path.basename(csv_path).replace(
                self.file_pattern.replace("*", ""), ""
            )

            # basic date handling - adjust format if needed
            if "month_year" not in df.columns:
                self.logger.error(
                    f"'month_year' column not found in {csv_path}. skipping."
                )
                return None
            df["month_year"] = pd.to_datetime(df["month_year"])
            df = df.set_index("month_year").sort_index()

            # check if target variable exists
            if self.target_variable not in df.columns:
                self.logger.error(
                    f"target variable '{self.target_variable}' not found in {csv_path}. skipping."
                )
                return None

            # drop rows with na in target
            # BY RIGHT! shouldn't have, since fixing was done in data prep within feature engineering
            df = df.dropna(subset=[self.target_variable])
            if df.empty:
                self.logger.warning(
                    f"no valid data after dropping na target for {country_name}. skipping."
                )
                return None

            # define features (x) and target (y)
            potential_features = [
                col for col in df.columns if col not in self.cols_to_drop
            ]
            # ensure all selected feature columns actually exist in the dataframe
            features = [f for f in potential_features if f in df.columns]

            if not features:
                self.logger.error(
                    f"no features selected or found for {country_name}. check cols_to_drop and csv columns. skipping."
                )
                return None

            x = df[features]
            y = df[self.target_variable]

            self.logger.info(f"features selected for {country_name}: {features}")
            return x, y, features

        except FileNotFoundError:
            self.logger.error(f"file not found: {csv_path}")
            return None
        except Exception as e:
            self.logger.error(f"failed to load or prepare data from {csv_path}: {e}")
            return None

    def _run_hyperopt(
        self,
        x: pd.DataFrame,
        y: pd.Series,
        features: List[str],
        country_name: str,
    ) -> Optional[Tuple[Dict[str, Any], Trials]]:
        """
        performs hyperparameter optimization using hyperopt and time series cv.

        args:
            x: feature dataframe.
            y: target series.
            features: list of feature names.
            country_name: name of the country for self.logger.

        returns:
            a tuple containing (best parameter dictionary, hyperopt trials object),
            or none if optimization fails.
        """
        if len(x) < self.n_cv_splits * 2:
            self.logger.warning(
                f"not enough data points ({len(x)}) for {self.n_cv_splits}-fold cv for {country_name}. skipping hp tuning."
            )
            return None

        tscv = TimeSeriesSplit(n_splits=self.n_cv_splits)

        def objective(params: Dict[str, Any]) -> Dict[str, Any]:
            """objective function for hyperopt to minimize (average cv rmse)."""
            params["max_depth"] = int(params["max_depth"])
            params["min_child_weight"] = int(params["min_child_weight"])
            cv_rmses = []
            # iterate through cv folds respecting time order
            for fold, (train_index, val_index) in enumerate(tscv.split(x)):
                x_train, x_val = x.iloc[train_index], x.iloc[val_index]
                y_train, y_val = y.iloc[train_index], y.iloc[val_index]

                dtrain = xgb.DMatrix(x_train, label=y_train, feature_names=features)
                dval = xgb.DMatrix(x_val, label=y_val, feature_names=features)
                watchlist = [(dtrain, "train"), (dval, "eval")]

                try:
                    model = xgb.train(
                        params,
                        dtrain,
                        num_boost_round=1000,  # high number, early stopping regulates
                        evals=watchlist,
                        early_stopping_rounds=self.early_stopping_rounds_cv,
                        verbose_eval=False,
                    )
                    preds = model.predict(dval)
                    rmse = np.sqrt(mean_squared_error(y_val, preds))
                    cv_rmses.append(rmse)
                except Exception as e:
                    # log occasional warnings, but don't stop unless it's severe
                    self.logger.debug(
                        f"warning during xgb.train in hyperopt cv for {country_name} fold {fold+1}: {e}"
                    )
                    # return high loss if a fold fails for these params
                    return {"loss": np.inf, "status": STATUS_OK}

            if not cv_rmses:  # if all folds failed
                return {"loss": np.inf, "status": STATUS_OK}

            avg_rmse = np.mean(cv_rmses)
            self.logger.debug(
                f"hyperopt eval for {country_name}: params={params}, avg_rmse={avg_rmse:.4f}"
            )
            return {"loss": avg_rmse, "status": STATUS_OK}

        # --- run optimization ---
        self.logger.info(f"[HP Tuning] starting hp optimization for {country_name}...")
        trials = Trials()
        try:
            best_result = fmin(
                fn=objective,
                space=self.search_space,
                algo=tpe.suggest,
                max_evals=self.max_hyperopt_evals,
                trials=trials,
                rstate=self.rng,
            )

            # construct the final parameter set
            # start with fixed params, update with best ones found by hyperopt
            final_params = self.search_space.copy()
            final_params.update(best_result)

            # ensure correct types for integer params after fmin
            final_params["max_depth"] = int(final_params["max_depth"])
            final_params["min_child_weight"] = int(final_params["min_child_weight"])

            # remove hp objects before training
            final_params = {
                k: v for k, v in final_params.items() if not hasattr(v, "pos_args")
            }
            # ensure core fixed params are correctly set
            final_params["objective"] = "reg:squarederror"
            final_params["eval_metric"] = "rmse"
            final_params["seed"] = self.seed

            best_loss = min(trials.losses()) if trials.losses() else float("inf")
            self.logger.info(
                f"[HP Results] best cross-validation rmse ({self.target_variable} scale): {best_loss:.4f}"
            )

            return final_params, trials

        except Exception as e:
            self.logger.error(
                f"hyperparameter optimization failed for {country_name}: {e}"
            )
            return None

    def _train_final_model(
        self,
        x: pd.DataFrame,
        y: pd.Series,
        features: List[str],
        final_params: Dict[str, Any],
        country_name: str,
    ) -> Optional[xgb.Booster]:
        """
        trains the final xgboost model using the best parameters and early stopping.

        args:
            x: full feature dataframe.
            y: full target series.
            features: list of feature names.
            final_params: the dictionary of best hyperparameters.
            country_name: name of the country for self.logger.

        returns:
            the trained xgboost booster object, or none if training fails.
        """
        self.logger.info(
            f"[Training] training final model for {country_name.upper()} using best parameters..."
        )

        # create a final train/val split from the entire country data for early stopping
        n_validation_points = max(1, int(len(x) * 0.15))  # use last 15% for validation
        x_train_final, x_val_final = (
            x.iloc[:-n_validation_points],
            x.iloc[-n_validation_points:],
        )
        y_train_final, y_val_final = (
            y.iloc[:-n_validation_points],
            y.iloc[-n_validation_points:],
        )

        # train with early stopping using the train/validation split
        dtrain_final_split = xgb.DMatrix(
            x_train_final, label=y_train_final, feature_names=features
        )
        dval_final_split = xgb.DMatrix(
            x_val_final, label=y_val_final, feature_names=features
        )
        watchlist_final = [
            (dtrain_final_split, "train"),
            (dval_final_split, "eval"),
        ]

        try:
            final_model = xgb.train(
                final_params,
                dtrain_final_split,
                num_boost_round=self.final_model_boost_rounds,
                evals=watchlist_final,
                early_stopping_rounds=self.early_stopping_rounds_final,
                verbose_eval=100,
            )
            return final_model
        except Exception as e:
            self.logger.error(f"final model training failed for {country_name}: {e}")
            return None

    def _save_artifacts(
        self,
        country_name: str,
        model: xgb.Booster,
        trials: Trials,
        params: Dict[str, Any],
    ):
        """
        saves the trained model, hyperopt trials, and best parameters.

        args:
            country_name: name of the country.
            model: the trained xgboost booster object.
            trials: the hyperopt trials object.
            params: the dictionary of best hyperparameters used.
        """
        model_filename = os.path.join(self.output_dir, f"{country_name}/xgb_model.json")
        trials_filename = os.path.join(
            self.output_dir, f"{country_name}/hyperopt_trials.pkl"
        )
        params_filename = os.path.join(
            self.output_dir, f"{country_name}/best_params.json"
        )

        try:
            # save model
            model.save_model(model_filename)
            # save trials
            with open(trials_filename, "wb") as f:
                joblib.dump(trials, f)
            # save parameters (ensure json serializable)
            serializable_params = {
                k: (
                    int(v)
                    if isinstance(v, (np.int_, np.integer))
                    else float(v) if isinstance(v, (np.float_, np.floating)) else v
                )
                for k, v in params.items()
            }
            with open(params_filename, "w") as f:
                json.dump(serializable_params, f, indent=4)
        except Exception as e:
            self.logger.error(f"failed to save artifacts for {country_name}: {e}")

    def train_for_country(self, csv_path: str):
        """
        orchestrates the full training pipeline for a single country's csv file.

        args:
            csv_path: the path to the country's csv file.
        """
        country_name = (
            os.path.basename(csv_path)
            .replace(self.file_pattern.replace("*", ""), "")
            .split("_")[0]
        )

        # make directory for country in output dir
        os.makedirs(os.path.join(self.output_dir, country_name), exist_ok=True)

        # 1. load data
        prep_result = self._load_and_prepare_data(csv_path)
        if prep_result is None:
            return
        x, y, features = prep_result

        # 2. run hp optimization
        hyperopt_result = self._run_hyperopt(x, y, features, country_name)
        if hyperopt_result is None:
            # for now, we skip if hyperopt fails/is skipped.
            self.logger.warning(
                f"skipping final model training for {country_name} due to hyperopt issues."
            )
            return
        best_params, trials = hyperopt_result

        # 3. train final model
        final_model = self._train_final_model(x, y, features, best_params, country_name)
        if final_model is None:
            return  # error logged in helper method

        # 4. save artifacts
        self._save_artifacts(country_name, final_model, trials, best_params)

    def run_training(self):
        """
        finds all country data files and runs the training pipeline for each.
        """
        self.logger.info("starting model training script.")
        all_files = glob.glob(os.path.join(self.data_path, self.file_pattern))
        self.logger.info(
            f"found {len(all_files)} potential country files in {self.data_path} matching '{self.file_pattern}'"
        )

        if not all_files:
            self.logger.warning("no files found. exiting.")
            return

        for f in all_files:
            self.train_for_country(f)

        self.logger.info("--- script finished ---")


if __name__ == "__main__":
    trainer = CountryModelTrainer()
    trainer.run_training()

    # you can now load models like this:
    # country = 'brunei' # example
    # model_path = os.path.join(trainer.output_dir, f"{country}_xgb_model.json")
    # if os.path.exists(model_path):
    #     loaded_model = xgb.Booster()
    #     loaded_model.load_model(model_path)
    #     print(f"loaded model for {country}")
    # # remember to load corresponding features for prediction
    # params_path = os.path.join(trainer.output_dir, f"{country}_best_params.json")
    # with open(params_path, 'r') as f:
    #     params = json.load(f)
    # # you might need feature list from data prep phase if loading separately
