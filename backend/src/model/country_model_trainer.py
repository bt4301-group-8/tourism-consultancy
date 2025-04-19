import os
import glob
import json
import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
import xgboost as xgb
import tempfile

from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll import scope
from typing import List, Dict, Tuple, Optional, Any
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

from backend.src.services import logger
import os

mlflow_tracking_uri = "http://localhost:9080"
mlflow_experiment_name = "Country Visitor Model Training"
mlflow.set_tracking_uri(mlflow_tracking_uri)
mlflow.set_experiment(mlflow_experiment_name)


class CountryModelTrainer:
    """
    trains an xgboost model for predicting visitor numbers for individual countries using mlflow.

    handles data loading, hyperparameter tuning using hyperopt, final model
    training, and logging artifacts (model, parameters, metrics, trials) to mlflow.
    """

    def __init__(
        self,
        data_path: str = "backend/data/countries/",  # temp, before migrating to db
        file_pattern: str = "*_final_df.csv",
        target_variable: str = "log_visitors",
        original_target: Optional[str] = "num_visitors",
        cols_to_drop: List[str] = [
            "month",
            "year",
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
            # todo: add any other columns that should not be features
        ],
        train_ratio: float = 0.75,
        val_ratio: float = 0.15,
        n_cv_splits: int = 5,
        max_hyperopt_evals: int = 50,
        early_stopping_rounds_cv: int = 25,
        early_stopping_rounds_final: int = 50,
        final_model_boost_rounds: int = 1500,
        seed: int = 42,
    ):
        """
        initializes the trainer with configuration settings.

        args:
            data_path: path to the directory containing processed country csv files.
            # output_dir: removed, artifacts saved to mlflow.
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

        self.logger = logger

    def _load_and_prepare_data(
        self,
        csv_path: str,
    ):
        """
        loads data, prepares features/target, and splits into train/val/test sets chronologically.

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
            # only keep features actually present in the dataframe
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
            # ensure all splits have at least one sample
            if n_train == 0 or n_val == 0 or (n - n_train - n_val) <= 0:
                self.logger.error(
                    f"cannot split data for {country_name} with {n} samples and ratios "
                    f"({self.train_ratio}, {self.val_ratio}). needs at least one sample per split. skipping."
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
    ) -> Optional[Tuple[Dict[str, Any], Trials, float]]:
        """
        performs hyperparameter optimization using hyperopt and time series cv on the training set.

        args:
            x_train: training feature dataframe.
            y_train: training target series.
            features: list of feature names.
            country_name: name of the country for self.logger.

        returns:
            a tuple containing (best parameter dictionary, hyperopt trials object, best cv rmse),
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
            params["max_depth"] = int(params["max_depth"])
            params["min_child_weight"] = int(params["min_child_weight"])
            cv_rmses = []
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
                    # return infinite loss if a fold fails
                    return {"loss": np.inf, "status": STATUS_OK}

            if (
                not cv_rmses
            ):  # handle case where all folds failed (though unlikely with above return)
                return {"loss": np.inf, "status": STATUS_OK}

            avg_rmse = np.mean(cv_rmses)
            return {"loss": avg_rmse, "status": STATUS_OK}

        self.logger.info(
            f"[hp tuning] starting hp optimization for {country_name} on training data..."
        )
        trials = Trials()
        try:
            best_result_values = fmin(
                fn=objective,
                space=self.search_space,
                algo=tpe.suggest,
                max_evals=self.max_hyperopt_evals,
                trials=trials,
                rstate=self.rng,
                show_progressbar=False,
            )

            # reconstruct the full best parameter dictionary
            # start with base parameters like objective, metric, seed
            final_params = {
                k: v
                for k, v in self.search_space.items()
                if not hasattr(v, "pos_args")  # filter out hp space definitions
            }
            # update with the best values found by fmin
            final_params.update(best_result_values)
            # ensure integer types are correct after fmin
            final_params["max_depth"] = int(final_params["max_depth"])
            final_params["min_child_weight"] = int(final_params["min_child_weight"])
            # ensure seed is set for final training consistency
            final_params["seed"] = self.seed

            best_loss = min(trials.losses()) if trials.losses() else float("inf")
            self.logger.info(
                f"[hp results] best cross-validation rmse ({self.target_variable} scale) on train data: {best_loss:.4f}"
            )

            return final_params, trials, best_loss

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
            f"[training] training final model for {country_name.upper()} using best parameters..."
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
                early_stopping_rounds=self.early_stopping_rounds_final,
                verbose_eval=100,
            )
            # log final validation score achieved during training
            self.logger.info(
                f"[training] final model training completed. best iteration: {final_model.best_iteration}, "
                f"best val rmse: {final_model.best_score:.4f}"
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
            f"[evaluation] evaluating final model for {country_name.upper()} on test set ({len(x_test)} samples)..."
        )
        try:
            dtest = xgb.DMatrix(x_test, label=y_test, feature_names=features)
            preds_transformed = model.predict(
                dtest
            )  # predictions are on the transformed scale
            rmse_transformed = np.sqrt(mean_squared_error(y_test, preds_transformed))

            metrics = {f"test_rmse_{self.target_variable}": rmse_transformed}

            # calculate rmse on original scale if possible
            if (
                self.original_target
                and self.original_target in df_test_original.columns
                and self.target_variable.startswith("log_")
            ):
                try:
                    # ensure index alignment when selecting original target values
                    y_test_original = df_test_original.loc[
                        y_test.index, self.original_target
                    ]

                    preds_original = np.exp(preds_transformed)
                    valid_indices = ~np.isnan(y_test_original) & ~np.isinf(
                        y_test_original
                    )
                    if valid_indices.sum() < len(y_test_original):
                        self.logger.warning(
                            f"  found nan/inf in original test target '{self.original_target}'. evaluating on valid subset."
                        )

                    if valid_indices.sum() > 0:
                        rmse_original = np.sqrt(
                            mean_squared_error(
                                y_test_original[valid_indices],
                                preds_original[valid_indices],
                            )
                        )
                        metrics[f"test_rmse_{self.original_target}"] = rmse_original
                        self.logger.info(
                            f"  test rmse ({self.original_target}): {rmse_original:.2f}"
                        )
                    else:
                        self.logger.warning(
                            f"  no valid original test target values found for {country_name} to calculate original scale rmse."
                        )

                except Exception as e_orig:
                    self.logger.warning(
                        f"[evaluation] could not calculate rmse on original scale ({self.original_target}) for {country_name}: {e_orig}"
                    )
            elif not self.target_variable.startswith("log_"):
                self.logger.info(
                    f"  target variable '{self.target_variable}' does not appear log-transformed. skipping original scale calculation."
                )
            else:
                self.logger.info(
                    f"  original target '{self.original_target}' not specified or not found in test data. skipping original scale rmse."
                )

            return metrics

        except Exception as e:
            self.logger.error(f"evaluation failed for {country_name}: {e}")
            return None

    def train_for_country(self, csv_path: str):
        """
        orchestrates the full training pipeline for a single country's csv file,
        logging results and artifacts to mlflow within a dedicated run.

        args:
            csv_path: the path to the country's csv file.
        """
        country_name = (
            os.path.basename(csv_path)
            .replace(self.file_pattern.replace("*", ""), "")
            .split("_")[0]
        )
        self.logger.info(
            f"--- starting training pipeline for {country_name.upper()} ---"
        )

        # 1. load and split data
        prep_result = self._load_and_prepare_data(csv_path)
        if prep_result is None:
            self.logger.error(
                f"failed to load/prepare/split data for {country_name}. stopping."
            )
            # maybe log a failed run to mlflow? or just skip as done here.
            return
        x_train, y_train, x_val, y_val, x_test, y_test, features, df_test_original = (
            prep_result
        )

        # start mlflow run for this country
        with mlflow.start_run(run_name=f"{country_name}_training") as run:
            run_id = run.info.run_id  # get run id
            # log country as a tag for easier filtering in ui
            mlflow.set_tag("country", country_name)
            mlflow.set_tag("target_variable", self.target_variable)
            mlflow.log_param("data_file", os.path.basename(csv_path))
            mlflow.log_param("train_ratio", self.train_ratio)
            mlflow.log_param("val_ratio", self.val_ratio)
            mlflow.log_param("test_ratio", self.test_ratio)
            mlflow.log_param("n_cv_splits_hp", self.n_cv_splits)
            mlflow.log_param("max_hyperopt_evals", self.max_hyperopt_evals)
            mlflow.log_param("early_stopping_rounds_cv", self.early_stopping_rounds_cv)
            mlflow.log_param(
                "early_stopping_rounds_final", self.early_stopping_rounds_final
            )
            mlflow.log_param("random_seed", self.seed)

            # log features list as an artifact (json)
            test_csv = "test_set.csv"
            # df_test_original already has your test rows (including original num_visitors)
            df_test_original.to_csv(test_csv, index=False)
            mlflow.log_artifact(test_csv, artifact_path="test_data")
            os.remove(test_csv)

            # log features list as an artifact (json)
            try:
                features_dict = {"features": features}
                mlflow.log_dict(features_dict, "features.json")
            except Exception as e:
                self.logger.warning(
                    f"could not log features list for {country_name}: {e}"
                )

            # calculate and log training data statistics for drift detection
            try:
                train_stats = x_train.describe().to_dict()
                mlflow.log_dict(train_stats, "training_feature_stats.json")
                self.logger.info(
                    f"[monitoring] logged training feature statistics for {country_name}"
                )
            except Exception as e:
                self.logger.warning(
                    f"could not calculate or log training feature stats for {country_name}: {e}"
                )

            # log a sample of training data for drift detection
            try:
                # determine feature types (categorical vs continuous)
                feature_types = {}
                for feature in features:
                    if x_train[
                        feature
                    ].dtype == "object" or pd.api.types.is_categorical_dtype(
                        x_train[feature]
                    ):
                        feature_types[feature] = "categorical"
                    elif len(x_train[feature].unique()) < 5 and x_train[
                        feature
                    ].dtype in ["int64", "int32"]:
                        # integers with few unique values are likely categorical
                        feature_types[feature] = "categorical"
                    else:
                        feature_types[feature] = "continuous"

                # log feature types for drift detection
                mlflow.log_dict(feature_types, "feature_types.json")

                # save a sample of training data (up to 1000 rows to keep size manageable)
                sample_size = min(1000, len(x_train))
                training_sample = x_train.sample(n=sample_size, random_state=self.seed)
                training_sample.to_csv("training_data_sample.csv", index=False)
                mlflow.log_artifact("training_data_sample.csv")
                os.remove("training_data_sample.csv")

                self.logger.info(
                    f"[monitoring] logged training data sample ({sample_size} rows) for {country_name}"
                )
            except Exception as e:
                self.logger.warning(
                    f"could not log training data sample for {country_name}: {e}"
                )

            # 2. run hp optimization (on train set)
            hyperopt_result = self._run_hyperopt(
                x_train, y_train, features, country_name
            )
            if hyperopt_result is None:
                self.logger.warning(
                    f"skipping final model training for {country_name} due to hyperopt issues or insufficient data."
                )
                mlflow.log_metric("training_status", 0)
                mlflow.set_tag("status", "skipped_hyperopt")
                return

            best_params, trials, _ = hyperopt_result

            # log best hyperopt parameters and the best cv score
            mlflow.log_params(best_params)

            # log hyperopt trials object as artifact
            try:
                # use tempfile to save trials before logging to mlflow (apparently this is best practice)
                with tempfile.NamedTemporaryFile(
                    suffix=".pkl", delete=False
                ) as tmp_file:
                    import joblib

                    joblib.dump(trials, tmp_file.name)
                    mlflow.log_artifact(tmp_file.name, artifact_path="hyperopt_trials")
                os.remove(tmp_file.name)  # clean up temp file
            except Exception as e:
                self.logger.warning(
                    f"could not log hyperopt trials for {country_name}: {e}"
                )

            # 3. train final model (on train, validate on val)
            final_model = self._train_final_model(
                x_train, y_train, x_val, y_val, features, best_params, country_name
            )
            if final_model is None:
                self.logger.error(
                    f"failed to train final model for {country_name}. stopping."
                )
                mlflow.log_metric("training_status", 0)  # indicate failure
                mlflow.set_tag("status", "failed_final_training")
                return

            # log the validation score achieved during final training
            mlflow.log_metric("final_model_best_val_rmse", final_model.best_score)

            # 4. evaluate final model (on test set)
            eval_metrics = self._evaluate_model(
                final_model, x_test, y_test, features, df_test_original, country_name
            )
            if eval_metrics:
                mlflow.log_metrics(eval_metrics)

            # 5. log the final model using mlflow's xgboost
            try:
                signature = infer_signature(
                    x_train, final_model.predict(xgb.DMatrix(x_train))
                )
            except Exception as e:
                self.logger.warning(
                    f"could not infer model signature for {country_name}: {e}"
                )
                signature = None  # log model without signature if inference fails

            try:
                mlflow.xgboost.log_model(
                    xgb_model=final_model,
                    artifact_path="model",  # subdirectory within the run's artifacts
                    signature=signature,
                    registered_model_name=f"{country_name}_visitor_model",
                    metadata={"run_id": run_id},
                )
            except Exception as e:
                self.logger.error(
                    f"failed to log model to mlflow for {country_name}: {e}"
                )
                mlflow.log_metric("training_status", 0)
                mlflow.set_tag("status", "failed_model_logging")
                return

            # if we reach here, the run was successful
            mlflow.log_metric("training_status", 1)
            mlflow.set_tag("status", "completed")
            self.logger.info(
                f"--- finished training pipeline for {country_name.upper()} ---"
            )

    def run_training(self):
        """
        finds all country data files and runs the training pipeline for each,
        logging each country to a separate mlflow run.
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
                processed_count += 1  # count attempts
            except Exception as e:
                # this catches errors outside the main train_for_country logic or before mlflow run starts
                country_name = os.path.basename(f).split("_")[0]
                self.logger.error(
                    f"unexpected error during training setup for {country_name} ({f}): {e}",
                    exc_info=True,
                )
                skipped_count += 1

        self.logger.info(f"--- overall training process finished ---")
        self.logger.info(f"attempted training for: {processed_count} countries.")
        if skipped_count > 0:
            self.logger.warning(
                f"skipped {skipped_count} countries due to errors before mlflow run start."
            )

    def train_incrementally_for_country(self, csv_path: str, min_months: int = 1):
        """
        Performs incremental training for a single country's CSV data.
        Each iteration trains with increasing months of data and evaluates on the fixed test set.
        Logs evaluation metrics for each training window to MLflow.

        Args:
            csv_path: Path to the country's CSV file.
            min_months: Minimum number of months to start training with.
        """
        country_name = os.path.basename(csv_path).split("_")[0]
        self.logger.info(
            f"--- starting incremental training for {country_name.upper()} ---"
        )

        try:
            df = pd.read_csv(csv_path)
            df["month_year"] = pd.to_datetime(df["month_year"])
            df = df.set_index("month_year").sort_index()

            # filter usable columns
            if self.target_variable not in df.columns:
                self.logger.error(
                    f"Target variable {self.target_variable} not in data for {country_name}"
                )
                return

            df = df.dropna(subset=[self.target_variable])
            if df.empty:
                self.logger.warning(f"No valid data in {csv_path}")
                return

            features = [
                col
                for col in df.columns
                if col not in self.cols_to_drop and col != self.target_variable
            ]
            if not features:
                self.logger.error(f"No valid features found for {country_name}")
                return

            # split into trainval and test (fixed test set from last N months)
            n_total = len(df)
            n_test = int(n_total * self.test_ratio)
            df_trainval = df.iloc[:-n_test]
            df_test = df.iloc[-n_test:]

            x_test = df_test[features]
            y_test = df_test[self.target_variable]
            df_test_original = df_test.copy()

            results = []

            for i in range(min_months, len(df_trainval) + 1, 5):
                df_subset = df_trainval.iloc[:i]
                x_train = df_subset[features]
                y_train = df_subset[self.target_variable]

                dtrain = xgb.DMatrix(x_train, label=y_train, feature_names=features)
                dtest = xgb.DMatrix(x_test, label=y_test, feature_names=features)

                run_name = f"{country_name}_incremental_{i}_months"
                with mlflow.start_run(run_name=run_name, nested=True):
                    mlflow.set_tag("country", country_name)
                    mlflow.set_tag("target_variable", self.target_variable)
                    mlflow.set_tag("status", "incremental")
                    mlflow.log_param("training_months", i)
                    mlflow.log_param("test_samples", len(df_test))
                    mlflow.log_param("random_seed", self.seed)

                    # Train model with default or simplified params
                    params = {
                        "objective": "reg:squarederror",
                        "eval_metric": "rmse",
                        "eta": 0.1,
                        "max_depth": 5,
                        "seed": self.seed,
                    }

                    model = xgb.train(
                        params,
                        dtrain,
                        num_boost_round=300,
                        evals=[(dtrain, "train")],
                        early_stopping_rounds=10,
                        verbose_eval=False,
                    )

                    # Evaluate
                    preds_log = model.predict(dtest)
                    rmse_log = np.sqrt(mean_squared_error(y_test, preds_log))
                    mlflow.log_metric("rmse_log", rmse_log)

                    result_row = {"months": i, "rmse_log": rmse_log}

                    if (
                        self.original_target
                        and self.original_target in df_test_original.columns
                    ):
                        try:
                            y_test_orig = df_test_original[self.original_target]
                            preds_orig = np.exp(preds_log)
                            rmse_orig = np.sqrt(
                                mean_squared_error(y_test_orig, preds_orig)
                            )
                            mlflow.log_metric("rmse_original", rmse_orig)
                            result_row["rmse_original"] = rmse_orig
                        except Exception as e:
                            self.logger.warning(f"Failed to compute original RMSE: {e}")

                    results.append(result_row)

            # Log RMSE vs Months plot
            result_df = pd.DataFrame(results)


            # Add country column to identify rows
            result_df["country"] = country_name

            # Define global output path
            summary_csv_path = os.path.join("results", "incremental_rmse_summary.csv")

            # Create directory if needed
            os.makedirs(os.path.dirname(summary_csv_path), exist_ok=True)

            # Append to CSV
            if not os.path.exists(summary_csv_path):
                result_df.to_csv(summary_csv_path, index=False)
            else:
                result_df.to_csv(summary_csv_path, mode="a", index=False, header=False)
            import matplotlib.pyplot as plt

            plt.figure(figsize=(10, 6))
            plt.plot(
                result_df["months"], result_df["rmse_log"], marker="o", label="Log RMSE"
            )
            # if "rmse_original" in result_df.columns:
            #     plt.plot(
            #         result_df["months"],
            #         result_df["rmse_original"],
            #         marker="o",
            #         label="Original RMSE",
            #     )
            plt.xlabel("Months of Training Data")
            plt.ylabel("RMSE")
            plt.title(f"Performance vs Training Window: {country_name}")
            plt.legend()
            plt.grid(True)

            fig_path = f"{country_name}_rmse_curve.png"
            plt.savefig(fig_path)
            mlflow.log_artifact(fig_path)
            os.remove(fig_path)

            self.logger.info(
                f"--- finished incremental training for {country_name.upper()} ---"
            )

        except Exception as e:
            self.logger.error(
                f"Incremental training failed for {country_name}: {e}", exc_info=True
            )

    def run_iterative_training(self):
        """
        finds all country data files and runs the training pipeline for each,
        logging each country to a separate mlflow run.
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
                self.train_incrementally_for_country(f)
                processed_count += 1  # count attempts
            except Exception as e:
                # this catches errors outside the main train_for_country logic or before mlflow run starts
                country_name = os.path.basename(f).split("_")[0]
                self.logger.error(
                    f"unexpected error during training setup for {country_name} ({f}): {e}",
                    exc_info=True,
                )
                skipped_count += 1

        self.logger.info(f"--- overall training process finished ---")
        self.logger.info(f"attempted training for: {processed_count} countries.")
        if skipped_count > 0:
            self.logger.warning(
                f"skipped {skipped_count} countries due to errors before mlflow run start."
            )

    def monitor_input_drift(
        self,
        run_id: str,
        new_data: pd.DataFrame,
        alpha: float = 0.05,
    ) -> Dict[str, bool]:
        """
        performs drift detection by comparing distributions of new data against training data
        using kolmogorov-smirnov test for continuous variables and chi-squared test for categorical ones.

        args:
            run_id: the mlflow run id corresponding to the trained model.
            new_data: a pandas dataframe containing new input features.
            alpha: significance level for drift detection (default 0.05).

        returns:
            a dictionary where keys are feature names and values are boolean indicating
            if drift was detected for that feature based on statistical tests.
        """
        from scipy.stats import ks_2samp, chisquare
        import time

        self.logger.info(
            f"[monitoring] starting input drift check for run_id: {run_id}"
        )
        drift_detected = {}
        client = MlflowClient()

        # add a timeout for artifact download
        max_wait_seconds = 30
        start_time = time.time()

        try:
            # download with timeout tracking
            while time.time() - start_time < max_wait_seconds:
                try:
                    # get artifact download paths
                    artifact_dir = client.download_artifacts(run_id, "")
                    training_data_path = None
                    feature_types_path = None

                    for root, dirs, files in os.walk(artifact_dir):
                        for f in files:
                            if f == "training_data_sample.csv":
                                training_data_path = os.path.join(root, f)
                            elif f == "feature_types.json":
                                feature_types_path = os.path.join(root, f)

                    missing_files = []
                    if not training_data_path:
                        missing_files.append("training_data_sample.csv")
                    if not feature_types_path:
                        missing_files.append("feature_types.json")

                    if missing_files:
                        raise FileNotFoundError(
                            f"Could not find required files: {', '.join(missing_files)}"
                        )

                    break
                except Exception as e:
                    if time.time() - start_time >= max_wait_seconds:
                        raise TimeoutError(
                            f"download timed out after {max_wait_seconds}s: {e}"
                        )
                    time.sleep(1)

            # read data and feature types
            train_data = pd.read_csv(training_data_path)

            with open(feature_types_path) as f:
                feature_types = json.load(f)

            # test features present in both datasets
            common_features = [
                col for col in train_data.columns if col in new_data.columns
            ]

            # test each feature
            for feature in common_features:
                # skip features with missing data
                if (
                    train_data[feature].isnull().any()
                    or new_data[feature].isnull().any()
                ):
                    self.logger.warning(
                        f"[monitoring] feature '{feature}' has null values, skipping"
                    )
                    drift_detected[feature] = False
                    continue

                # appropriate test based on feature type
                if feature_types.get(feature) == "categorical":
                    try:
                        train_counts = train_data[feature].value_counts()
                        new_counts = new_data[feature].value_counts()

                        common_categories = set(train_counts.index).intersection(
                            set(new_counts.index)
                        )
                        if len(common_categories) < 2:
                            self.logger.warning(
                                f"[monitoring] not enough common categories for '{feature}'"
                            )
                            drift_detected[feature] = False
                            continue

                        # arrays for chi-squared test
                        observed = np.array(
                            [new_counts.get(cat, 0) for cat in common_categories]
                        )
                        expected_ratio = sum(new_counts) / sum(train_counts)
                        expected = np.array(
                            [
                                train_counts.get(cat, 0) * expected_ratio
                                for cat in common_categories
                            ]
                        )

                        # chi-squared test and check p-value
                        chi2, p_value = chisquare(observed, expected)
                        drift_detected[feature] = p_value < alpha

                        if drift_detected[feature]:
                            self.logger.warning(
                                f"[monitoring] drift in categorical feature '{feature}', p={p_value:.6f}"
                            )

                    except Exception as e:
                        self.logger.warning(
                            f"[monitoring] chi-squared test failed for '{feature}': {e}"
                        )
                        drift_detected[feature] = False
                else:
                    # continuous features
                    try:
                        ks_stat, p_value = ks_2samp(
                            train_data[feature].values, new_data[feature].values
                        )
                        drift_detected[feature] = p_value < alpha

                        if drift_detected[feature]:
                            self.logger.warning(
                                f"[monitoring] drift in continuous feature '{feature}', p={p_value:.6f}"
                            )

                    except Exception as e:
                        self.logger.warning(
                            f"[monitoring] ks test failed for '{feature}': {e}"
                        )
                        drift_detected[feature] = False

            # results
            drifted_features = [f for f, drifted in drift_detected.items() if drifted]
            self.logger.info(
                f"[monitoring] drift check complete: {len(drifted_features)}/{len(common_features)} features drifted"
            )

        except Exception as e:
            self.logger.error(
                f"[monitoring] drift detection failed: {e}", exc_info=True
            )
            return {}

        return drift_detected


if __name__ == "__main__":
    # ensure mlflow uri and experiment are set before running
    # (already done globally above here)
    trainer = CountryModelTrainer()
    trainer.run_training()
