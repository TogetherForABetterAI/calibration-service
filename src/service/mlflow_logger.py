import numpy as np
import pandas as pd
import mlflow
import os
import logging
from lib.config import config_params
from typing import List

class MlflowLogger:
    def __init__(self, client_id: str = None):
        artifacts_path = os.path.join(os.getcwd(), config_params["artifacts_path"])
        os.makedirs(artifacts_path, exist_ok=True)

        # Create client-specific experiment name
        experiment_name = (
            f"{config_params['experiment_name']} - Client {client_id}"
            if client_id
            else config_params["experiment_name"]
        )
        mlflow.set_experiment(experiment_name)

        self._artifacts_path = artifacts_path
        self._client_id = client_id
        # Create client-specific run name
        run_name = (
            f"EvaluationSession-{client_id}" if client_id else "EvaluationSession"
        )
        self._mlflow_run = mlflow.start_run(run_name=run_name)

        client_log = f" for client {client_id}" if client_id else ""
        logging.info(f"[MLflow] Started global evaluation run{client_log}")
 
    def log_single_batch(self, batch_index: int, probs: np.ndarray, inputs: np.ndarray, labels: List[int]):
        input_flat = inputs.reshape(inputs.shape[0], -1).tolist()
        probs_list = probs.tolist()

        logging.info(
            f"[MLflow] Logging batch {batch_index} with {len(probs_list)} probabilities and {len(input_flat)} inputs"
        )

        df = pd.DataFrame({"input": input_flat, "y_pred": probs_list, "y_test": labels})

        # Create client-specific filename
        client_suffix = f"-{self._client_id}" if self._client_id else ""
        filename = f"batch{batch_index}{client_suffix}.parquet"
        file_path = os.path.join(self._artifacts_path, filename)
        df.to_parquet(file_path, index=False)

        # Create client-specific artifact path
        artifact_path = f"batches/{self._client_id}" if self._client_id else "batches"
        mlflow.log_artifact(file_path, artifact_path=artifact_path)
        os.remove(file_path)

        client_log = f" for client {self._client_id}" if self._client_id else ""
        logging.info(
            f"[MLflow] Logged batch {batch_index}{client_log} to '{artifact_path}/{filename}'"
        )

    def end_run(self):
        mlflow.end_run()
        client_log = f" for client {self._client_id}" if self._client_id else ""
        logging.info(f"[MLflow] Run ended{client_log}")

