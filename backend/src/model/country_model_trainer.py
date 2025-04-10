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
        train_ratio: float = 0.75,  # portion of data for training
        val_ratio: float = 0.15,  # portion of data for validation
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
            train_ratio: proportion of data for the training set.
            val_ratio: proportion of data for the validation set.
            n_cv_splits: number of folds for time series cross-validation during hyperopt (on train set).
            max_hyperopt_evals: maximum number of hyperparameter evaluations.
            early_stopping_rounds_cv: early stopping rounds for xgb during hyperopt cv folds.
            early_stopping_rounds_final: early stopping rounds for final model training (using val set).
            final_model_boost_rounds: max boost rounds for final model (early stopping determines actual).
            seed: random seed for reproducibility.
        """
        self.data_path = data_path
        self.output_dir = output_dir
        self.file_pattern = file_pattern
        self.target_variable = target_variable
        self.cols_to_drop = list(set(cols_to_drop + [target_variable]))
        self.original_target = original_target
        self.train_ratio = train_ratio
        self.val_ratio = val_ratio
        self.test_ratio = 1.0 - train_ratio - val_ratio
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
    ):
        """
        loads data, prepares features/target, and splits into train/val/test sets CHRONOLOGICALLY (to adhere to time)

        args:
            csv_path: path to the country's csv file.

        returns:
            a tuple containing (x_train, y_train, x_val, y_val, x_test, y_test, feature_names, df_test_original)
            or none if loading/preparation/splitting fails. df_test_original contains original test data
            including the non-transformed target for evaluation.
        """
        try:
            df = pd.read_csv(csv_path)
            country_name = (
                os.path.basename(csv_path)
                .replace(self.file_pattern.replace("*", ""), "")
                .split("_")[0]
            )

            if "month_year" not in df.columns:
                self.logger.error(
                    f"'month_year' column not found in {csv_path}. skipping."
                )
                return None
            df["month_year"] = pd.to_datetime(df["month_year"])
            df = df.set_index("month_year").sort_index()

            if self.target_variable not in df.columns:
                self.logger.error(
                    f"target variable '{self.target_variable}' not found in {csv_path}. skipping."
                )
                return None

            df = df.dropna(subset=[self.target_variable])
            if df.empty:
                self.logger.warning(
                    f"no valid data after dropping na target for {country_name}. skipping."
                )
                return None

            potential_features = [
                col for col in df.columns if col not in self.cols_to_drop
            ]
            features = [f for f in potential_features if f in df.columns]

            if not features:
                self.logger.error(
                    f"no features selected or found for {country_name}. check cols_to_drop and csv columns. skipping."
                )
                return None

            x = df[features]
            y = df[self.target_variable]

            # split data chronologically
            n = len(df)
            n_train = int(n * self.train_ratio)
            n_val = int(n * self.val_ratio)
            if n_train == 0 or n_val == 0 or (n - n_train - n_val) == 0:
                self.logger.error(
                    f"cannot split data for {country_name} with {n} samples and ratios "
                    f"({self.train_ratio}, {self.val_ratio}). skipping."
                )
                return None

            train_end_idx = n_train
            val_end_idx = n_train + n_val

            x_train, y_train = x.iloc[:train_end_idx], y.iloc[:train_end_idx]
            x_val, y_val = (
                x.iloc[train_end_idx:val_end_idx],
                y.iloc[train_end_idx:val_end_idx],
            )
            x_test, y_test = x.iloc[val_end_idx:], y.iloc[val_end_idx:]

            # keep original test data (including original target) for evaluation
            df_test_original = df.iloc[val_end_idx:].copy()

            return (
                x_train,
                y_train,
                x_val,
                y_val,
                x_test,
                y_test,
                features,
                df_test_original,
            )

        except FileNotFoundError:
            self.logger.error(f"file not found: {csv_path}")
            return None
        except Exception as e:
            self.logger.error(f"failed to load/prepare/split data from {csv_path}: {e}")
            return None

    def _run_hyperopt(
        self,
        x_train: pd.DataFrame,
        y_train: pd.Series,
        features: List[str],
        country_name: str,
    ) -> Optional[Tuple[Dict[str, Any], Trials]]:
        """
        performs hyperparameter optimization using hyperopt and time series cv on the training set.

        args:
            x_train: training feature dataframe.
            y_train: training target series.
            features: list of feature names.
            country_name: name of the country for self.logger.

        returns:
            a tuple containing (best parameter dictionary, hyperopt trials object),
            or none if optimization fails or data is insufficient.
        """
        if len(x_train) < self.n_cv_splits * 2:
            self.logger.warning(
                f"not enough training data points ({len(x_train)}) for {self.n_cv_splits}-fold cv "
                f"for {country_name}. skipping hp tuning."
            )
            return None

        tscv = TimeSeriesSplit(n_splits=self.n_cv_splits)

        def objective(params: Dict[str, Any]) -> Dict[str, Any]:
            """objective function for hyperopt to minimize (average cv rmse on train folds)."""
            params["max_depth"] = int(params["max_depth"])
            params["min_child_weight"] = int(params["min_child_weight"])
            cv_rmses = []
            # iterate through cv folds on the training data
            for fold, (train_fold_index, val_fold_index) in enumerate(
                tscv.split(x_train)
            ):
                x_train_fold, x_val_fold = (
                    x_train.iloc[train_fold_index],
                    x_train.iloc[val_fold_index],
                )
                y_train_fold, y_val_fold = (
                    y_train.iloc[train_fold_index],
                    y_train.iloc[val_fold_index],
                )

                dtrain = xgb.DMatrix(
                    x_train_fold, label=y_train_fold, feature_names=features
                )
                dval = xgb.DMatrix(x_val_fold, label=y_val_fold, feature_names=features)
                watchlist = [(dtrain, "train"), (dval, "eval")]

                try:
                    model = xgb.train(
                        params,
                        dtrain,
                        num_boost_round=1000,
                        evals=watchlist,
                        early_stopping_rounds=self.early_stopping_rounds_cv,
                        verbose_eval=False,
                    )
                    preds = model.predict(dval)
                    rmse = np.sqrt(mean_squared_error(y_val_fold, preds))
                    cv_rmses.append(rmse)
                except Exception as e:
                    self.logger.debug(
                        f"warning during xgb.train in hyperopt cv for {country_name} fold {fold+1}: {e}"
                    )
                    return {"loss": np.inf, "status": STATUS_OK}

            if not cv_rmses:
                return {"loss": np.inf, "status": STATUS_OK}

            avg_rmse = np.mean(cv_rmses)
            # log less frequently during hyperopt to avoid clutter
            # self.logger.debug(f"hyperopt eval for {country_name}: params={params}, avg_rmse={avg_rmse:.4f}")
            return {"loss": avg_rmse, "status": STATUS_OK}

        self.logger.info(
            f"[HP Tuning] starting hp optimization for {country_name} on training data..."
        )
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

            final_params = self.search_space.copy()
            final_params.update(best_result)
            final_params["max_depth"] = int(final_params["max_depth"])
            final_params["min_child_weight"] = int(final_params["min_child_weight"])
            final_params = {
                k: v for k, v in final_params.items() if not hasattr(v, "pos_args")
            }
            final_params["objective"] = "reg:squarederror"
            final_params["eval_metric"] = "rmse"
            final_params["seed"] = self.seed

            best_loss = min(trials.losses()) if trials.losses() else float("inf")
            self.logger.info(
                f"[HP Results] best cross-validation rmse ({self.target_variable} scale) on train data: {best_loss:.4f}"
            )

            return final_params, trials

        except Exception as e:
            self.logger.error(
                f"hyperparameter optimization failed for {country_name}: {e}"
            )
            return None

    def _train_final_model(
        self,
        x_train: pd.DataFrame,
        y_train: pd.Series,
        x_val: pd.DataFrame,
        y_val: pd.Series,
        features: List[str],
        final_params: Dict[str, Any],
        country_name: str,
    ) -> Optional[xgb.Booster]:
        """
        trains the final xgboost model using the best parameters, training on the
        train set and using the validation set for early stopping.

        args:
            x_train: training feature dataframe.
            y_train: training target series.
            x_val: validation feature dataframe.
            y_val: validation target series.
            features: list of feature names.
            final_params: the dictionary of best hyperparameters.
            country_name: name of the country for self.logger.

        returns:
            the trained xgboost booster object, or none if training fails.
        """
        self.logger.info(
            f"[Training] training final model for {country_name.upper()} using best parameters..."
        )
        # create dmatrices for train and validation
        dtrain = xgb.DMatrix(x_train, label=y_train, feature_names=features)
        dval = xgb.DMatrix(x_val, label=y_val, feature_names=features)
        watchlist = [
            (dtrain, "train"),
            (dval, "eval"),
        ]
        try:
            final_model = xgb.train(
                final_params,
                dtrain,
                num_boost_round=self.final_model_boost_rounds,
                evals=watchlist,
                early_stopping_rounds=self.early_stopping_rounds_final,  # use validation set for stopping
                verbose_eval=100,  # log progress periodically
            )
            self.logger.info(
                f"[Training] final model training completed. best iteration: {final_model.best_iteration}\n"
                f"[Training] best val rmse: {final_model.best_score:.4f}"
            )
            return final_model
        except Exception as e:
            self.logger.error(f"final model training failed for {country_name}: {e}")
            return None

    def _evaluate_model(
        self,
        model: xgb.Booster,
        x_test: pd.DataFrame,
        y_test: pd.Series,
        features: List[str],
        df_test_original: pd.DataFrame,
        country_name: str,
    ) -> Optional[Dict[str, float]]:
        """
        evaluates the trained model on the held-out test set.

        calculates rmse on the target scale (e.g., log_visitors) and optionally
        on the original scale (e.g., num_visitors) if original_target is specified.

        args:
            model: the trained xgboost booster object.
            x_test: test feature dataframe.
            y_test: test target series (transformed scale, e.g., log).
            features: list of feature names used by the model.
            df_test_original: dataframe containing the original test data, including the
                              non-transformed target column.
            country_name: name of the country for self.logger.

        returns:
            a dictionary containing evaluation metrics (e.g., {'test_rmse_log': value, 'test_rmse_original': value}),
            or none if evaluation fails.
        """
        self.logger.info(
            f"[Evaluation] evaluating final model for {country_name.upper()} on test set ({len(x_test)} samples)..."
        )
        try:
            dtest = xgb.DMatrix(x_test, label=y_test, feature_names=features)
            preds_log = model.predict(dtest)
            rmse_log = np.sqrt(mean_squared_error(y_test, preds_log))

            metrics = {f"test_rmse_{self.target_variable}": rmse_log}
            self.logger.info(f"  test rmse ({self.target_variable}): {rmse_log:.4f}")

            # calculate rmse on original scale if possible
            if (
                self.original_target
                and self.original_target in df_test_original.columns
            ):
                try:
                    y_test_original = df_test_original.loc[
                        y_test.index, self.original_target
                    ]

                    # inverse transform predictions and true values (assuming log transform)
                    # add check for negative predictions if log transform was used
                    preds_original = np.exp(preds_log)
                    rmse_original = np.sqrt(
                        mean_squared_error(y_test_original, preds_original)
                    )
                    metrics[f"test_rmse_{self.original_target}"] = rmse_original
                    self.logger.info(
                        f"  test rmse ({self.original_target}): {rmse_original:.2f}"
                    )
                except Exception as e_orig:
                    self.logger.warning(
                        f"[Evaluation] could not calculate rmse on original scale ({self.original_target}) for {country_name}: {e_orig}"
                    )
            else:
                self.logger.info(
                    f"[Evaluation] original target '{self.original_target}' not specified or not found in test data. skipping original scale rmse."
                )

            return metrics

        except Exception as e:
            self.logger.error(f"evaluation failed for {country_name}: {e}")
            return None

    def _save_artifacts(
        self,
        country_name: str,
        model: xgb.Booster,
        trials: Optional[Trials],
        params: Dict[str, Any],
        eval_metrics: Optional[Dict[str, float]],
    ):
        """
        saves the trained model, hyperopt trials, best parameters, and evaluation metrics.

        args:
            country_name: name of the country.
            model: the trained xgboost booster object.
            trials: the hyperopt trials object (can be none).
            params: the dictionary of best hyperparameters used.
            eval_metrics: dictionary containing test set evaluation metrics (can be none).
        """
        country_output_dir = os.path.join(self.output_dir, country_name)
        os.makedirs(country_output_dir, exist_ok=True)  # ensure dir exists

        model_filename = os.path.join(country_output_dir, "xgb_model.json")
        trials_filename = os.path.join(country_output_dir, "hyperopt_trials.pkl")
        params_filename = os.path.join(country_output_dir, "best_params.json")
        metrics_filename = os.path.join(country_output_dir, "evaluation_metrics.json")

        try:
            model.save_model(model_filename)

            # save trials if they exist
            if trials:
                with open(trials_filename, "wb") as f:
                    joblib.dump(trials, f)
            else:
                self.logger.info(
                    "no hyperopt trials to save (tuning might have been skipped)."
                )

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

            # save evaluation metrics if they exist
            if eval_metrics:
                with open(metrics_filename, "w") as f:
                    json.dump(eval_metrics, f, indent=4)
            else:
                self.logger.info("no evaluation metrics to save.")

        except Exception as e:
            self.logger.error(f"failed to save artifacts for {country_name}: {e}")

    def train_for_country(self, csv_path: str):
        """
        orchestrates the full training pipeline for a single country's csv file,
        including data splitting, hp tuning, training, evaluation, and saving.

        args:
            csv_path: the path to the country's csv file.
        """
        country_name = (
            os.path.basename(csv_path)
            .replace(self.file_pattern.replace("*", ""), "")
            .split("_")[0]
        )
        # 1. load and split data
        prep_result = self._load_and_prepare_data(csv_path)
        if prep_result is None:
            self.logger.error(
                f"failed to load/prepare/split data for {country_name}. stopping."
            )
            return
        x_train, y_train, x_val, y_val, x_test, y_test, features, df_test_original = (
            prep_result
        )

        # 2. run hp optimization (on train set)
        hyperopt_result = self._run_hyperopt(x_train, y_train, features, country_name)
        if hyperopt_result is None:
            self.logger.warning(
                f"skipping final model training for {country_name} due to hyperopt issues or insufficient data."
            )
            # save dummy artifacts? or nothing? let's save nothing for now.
            return
        best_params, trials = hyperopt_result

        # 3. train final model (on train, validate on val)
        final_model = self._train_final_model(
            x_train, y_train, x_val, y_val, features, best_params, country_name
        )
        if final_model is None:
            self.logger.error(
                f"failed to train final model for {country_name}. stopping."
            )
            return

        # 4. evaluate final model (on test set)
        eval_metrics = self._evaluate_model(
            final_model, x_test, y_test, features, df_test_original, country_name
        )
        # 5. save artifacts (model, params, trials, metrics)
        self._save_artifacts(
            country_name, final_model, trials, best_params, eval_metrics
        )

        self.logger.info(
            f"--- finished training pipeline for {country_name.upper()} ---"
        )

    def run_training(self):
        """
        finds all country data files and runs the training pipeline for each.
        """
        all_files = glob.glob(os.path.join(self.data_path, self.file_pattern))
        self.logger.info(
            f"found {len(all_files)} country csvs in {self.data_path} matching '{self.file_pattern}'"
        )
        if not all_files:
            self.logger.warning("no files found. exiting.")
            return

        processed_count = 0
        skipped_count = 0
        for f in sorted(all_files):  # sort for consistent order
            try:
                self.train_for_country(f)
                processed_count += 1
            except Exception as e:
                country_name = os.path.basename(f).split("_")[0]
                self.logger.error(
                    f"unexpected error during training for {country_name} ({f}): {e}",
                    exc_info=True,
                )
                skipped_count += 1

        self.logger.info(f"--- overall training finished ---")
        self.logger.info(
            f"processed: {processed_count}, skipped/failed: {skipped_count}"
        )


if __name__ == "__main__":
    trainer = CountryModelTrainer()
    trainer.run_training()

    # you can now load models like this:
    # country = 'brunei' # example
    # model_path = os.path.join(trainer.output_dir, f"{country}/xgb_model.json")
    # if os.path.exists(model_path):
    #     loaded_model = xgb.Booster()
    #     loaded_model.load_model(model_path)
    #     print(f"loaded model for {country}")
    # # remember to load corresponding features for prediction
    # params_path = os.path.join(trainer.output_dir, f"{country}_best_params.json")
    # with open(params_path, 'r') as f:
    #     params = json.load(f)
    # # you might need feature list from data prep phase if loading separately
