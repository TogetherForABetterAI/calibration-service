import numpy as np
import pandas as pd
import mlflow
import os
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

class MlflowLogger:
    def __init__(self):
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        artifacts_path = os.path.join(project_root, "artifacts")
        os.makedirs(artifacts_path, exist_ok=True)

        mlflow.set_experiment("Calibration Experiment for MNIST")

        self._artifacts_path = artifacts_path
        self._batches = {}  

        self._mlflow_run = mlflow.start_run(run_name="EvaluationSession")
        logging.info("[MLflow] Started global evaluation run")

        self._eof_inputs_received = False
        self._eof_outputs_received = False

        self._all_labels = []
        self._all_probs = []

    def store_outputs(self, batch_index: int, probs: np.ndarray, is_last_batch: bool):
        self._store(batch_index, "probs", probs)

        if is_last_batch:
            self._eof_outputs_received = True
            self._try_end_run()

    def store_input_data(self, batch_index: int, inputs: np.ndarray, is_last_batch: bool, labels: np.ndarray):
        self._store(batch_index, "inputs", inputs)
        self._store(batch_index, "labels", labels)

        if is_last_batch:
            self._eof_inputs_received = True
            self._try_end_run()

    def _store(self, batch_index: int, kind: str, data: np.ndarray):
        if batch_index not in self._batches:
            self._batches[batch_index] = {"inputs": None, "probs": None, "labels": None}

        self._batches[batch_index][kind] = data

        entry = self._batches[batch_index]
        if entry["inputs"] is not None and entry["probs"] is not None and entry["labels"] is not None:
            self.log_single_batch(batch_index, entry["probs"], entry["inputs"], entry["labels"])
            del self._batches[batch_index]

    def log_single_batch(self, batch_index: int, probs: np.ndarray, inputs: np.ndarray, labels: np.ndarray):
        input_flat = inputs.reshape(inputs.shape[0], -1).tolist()
        probs_list = probs.tolist()
        labels_list = labels.tolist()

        df = pd.DataFrame({
            "input": input_flat,
            "probabilities": probs_list,
            "label": labels_list
        })

        filename = f"batch{batch_index}.parquet"
        file_path = os.path.join(self._artifacts_path, filename)
        df.to_parquet(file_path, index=False)

        mlflow.log_artifact(file_path, artifact_path="batches")
        os.remove(file_path)

        self._all_labels.extend(labels_list)
        self._all_probs.extend(probs)

        logging.info(f"[MLflow] Logged batch {batch_index} to 'batches/{filename}'")

    def _log_prediction_vs_truth(self):
        y_true = np.array(self._all_labels)
        probs = np.array(self._all_probs)
        probs_softmax = self._softmax(probs)
        y_pred = np.argmax(probs_softmax, axis=1)

        acc = np.mean(y_true == y_pred)
        logging.info(f"[MLflow] Final accuracy: {acc:.4f}")
        mlflow.log_metric("accuracy", acc)

        # Matriz de confusión
        cm = confusion_matrix(y_true, y_pred)
        disp = ConfusionMatrixDisplay(confusion_matrix=cm)
        disp.plot(cmap='Blues')

        fig_path = os.path.join(self._artifacts_path, "confusion_matrix.png")
        plt.title("Predictions vs True Labels")
        plt.savefig(fig_path)
        plt.close()

        mlflow.log_artifact(fig_path, artifact_path="final_analysis")
        os.remove(fig_path)

    def _softmax(self, logits):
        exp_logits = np.exp(logits - np.max(logits, axis=1, keepdims=True))
        return exp_logits / np.sum(exp_logits, axis=1, keepdims=True)

    def end_run(self):
        self._log_prediction_vs_truth()
        mlflow.end_run()
        logging.info("[MLflow] Run ended")

    def _try_end_run(self):
        if self._eof_inputs_received and self._eof_outputs_received:
            self.end_run()
