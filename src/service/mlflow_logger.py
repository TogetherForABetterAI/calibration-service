import uuid
import numpy as np
import mlflow
from pathlib import Path

class BatchLogger:
    def __init__(self):
        self.reset()

    def reset(self):
        self.batches = []
        self.run = None
        self.run_id = None

    def start_run(self):
        if self.run is None:
            self.run = mlflow.start_run()
            self.run_id = self.run.info.run_id
            print(f"[MLflow] Started run {self.run_id}")

    def log_batch(self, probs):
        self.start_run()
        self.batches.append(probs)

    def finalize(self):
        if self.run:
            Path("artifacts").mkdir(exist_ok=True)
            
            # Eliminar predicciones vacías (e.g. [] del eof)
            filtered_batches = [b for b in self.batches if b]
            all_probs = np.array(filtered_batches)

            output_path = Path("artifacts") / f"model_outputs_{self.run_id}.npy"
            np.save(output_path, all_probs)

            mlflow.log_artifact(str(output_path))
            mlflow.log_param("source", "batchwise_probs")
            mlflow.log_metric("num_batches", len(filtered_batches))
            mlflow.log_metric("total_samples", len(filtered_batches) * len(filtered_batches[0]) if filtered_batches else 0)

            mlflow.end_run()
            print(f"[MLflow] Finished run {self.run_id}")
            self.reset()
            return str(output_path)
        return None


batch_logger = BatchLogger()
