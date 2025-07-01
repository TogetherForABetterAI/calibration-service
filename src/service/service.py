import numpy as np
import mlflow
import os
import logging

class MlflowLogger:
    def __init__(self):
        project_root = os.path.dirname(
            os.path.dirname(os.path.dirname(__file__)))
        artifacts_path = os.path.join(project_root, "artifacts")
        os.makedirs(artifacts_path, exist_ok=True)
        
        mlflow.set_experiment("Calibration Experiment for MNIST")
        self._runs = []
        self._pending_pairs = {}
        self._artifacts_path = artifacts_path
        

    def store_outputs(self, msg_id, probs):
        if msg_id in self._pending_pairs:
            self.log_batches(probs, self._pending_pairs[msg_id])
            del self._pending_pairs[msg_id]
            return
    
        self._pending_pairs[msg_id] = probs
            
    def store_input_data(self, msg_id, input_data):
        if msg_id in self._pending_pairs:
            self.log_batches(self._pending_pairs[msg_id], input_data)
            del self._pending_pairs[msg_id]
            return
        
        self._pending_pairs[msg_id] = input_data
        
    def log_batches(self, probs, input_data):
        with mlflow.start_run() as run:
            run_id = run.info.run_id
            self._runs.append(run_id)
            
            run_id_path = os.path.join(self._artifacts_path, run_id)
            os.makedirs(run_id_path, exist_ok=True)
            logging.info(f"[MLflow] Started run {run_id}")
            
            probs_path = os.path.join(run_id_path, f"model_probs_{run_id}.npy")
            np.save(probs_path, probs)
            mlflow.log_artifact(str(probs_path))
            
            input_data_path = os.path.join(run_id_path, f"input_data_{run_id}.npy")
            np.save(input_data_path, input_data)
            mlflow.log_artifact(str(input_data_path))
            
            mlflow.log_metric("num_samples", len(input_data))
