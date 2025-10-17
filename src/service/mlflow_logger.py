import numpy as np
import pandas as pd
import mlflow
import os
import logging
from lib.config import initialize_config
from typing import List


class MlflowLogger:
    def __init__(self, mlflow_client: mlflow.MlflowClient, client_id: str = None):
        config = initialize_config().mlflow_config
        artifacts_path = os.path.join(os.getcwd(), config.artifacts_path)
        os.makedirs(artifacts_path, exist_ok=True)

        self._experiment = mlflow.set_experiment(
            f"{config.experiment_name} - Client {client_id}"
        )

        self._artifacts_path = artifacts_path
        self._client_id = client_id

        self._mlflow_client = mlflow_client
        self._mlflow_run = mlflow_client.create_run(
            experiment_id=self._experiment.experiment_id,
            run_name=f"EvaluationSession-{client_id}",
        )

        logging.info(f"[MLflow] Started global evaluation run for client {client_id}")

    def log_single_batch(
        self, batch_index: int, probs: np.ndarray, inputs: np.ndarray, labels: List[int]
    ):
        input_flat = inputs.reshape(inputs.shape[0], -1).tolist()
        probs_list = probs.tolist()

        logging.info(
            f"[MLflow] Logging batch {batch_index} with {len(probs_list)} probabilities and {len(input_flat)} inputs"
        )

        df = pd.DataFrame({"input": input_flat, "y_pred": probs_list, "y_test": labels})

        # Create client-specific filename
        filename = f"batch{batch_index}-{self._client_id}.parquet"
        file_path = os.path.join(self._artifacts_path, filename)
        df.to_parquet(file_path, index=False)

        # Create client-specific artifact path
        self._mlflow_client.log_artifact(
            run_id=self._mlflow_run.info.run_id,
            local_path=file_path,
            artifact_path=f"batches/{self._client_id}",
        )
        os.remove(file_path)

        logging.info(
            f"[MLflow] Logged batch {batch_index} for client {self._client_id} to 'batches/{self._client_id}/{filename}'"
        )

    def end_run(self):
        self._mlflow_client.set_terminated(self._mlflow_run.info.run_id)
        logging.info(f"[MLflow] Run ended for client {self._client_id}")
