import os
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import logging
from reportlab.lib.colors import grey
import base64
from email.message import EmailMessage

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from lib.config import config_params
import smtplib
import ssl
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import tempfile

class ReportBuilder:
    def __init__(self, client_id):
        self._probs = {} # Dictionary with {[] prob_outputs : int label}
        self._client_id = client_id
        self._canvas = canvas.Canvas(f"reports/{self._client_id}.pdf", pagesize=A4)

    def build_report(self, y_test, probs):
        logging.info(f"Generating report for client {self._client_id}")
        
        y_pred = np.argmax(probs, axis=1)
        confidences = np.max(probs, axis=1)
        logging.info(f"y_test: {y_test}")
        logging.info(f"y_pred: {y_pred}")
        logging.info(f"confidences: {confidences}")

        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average='weighted')
        recall = recall_score(y_test, y_pred, average='weighted')
        f1 = f1_score(y_test, y_pred, average='weighted')

        width, height = A4
        # Título principal
        self._canvas.setFont("Helvetica-Bold", 18)
        self._canvas.drawString(50, height - 50, "Conformal Predictor Report")

        # Subtítulo
        self._canvas.setFont("Helvetica", 12)
        self._canvas.drawString(50, height - 80, f"Client ID: {self._client_id}")

        # Línea separadora
        self._canvas.setStrokeColor(grey)
        self._canvas.setLineWidth(0.5)
        self._canvas.line(50, height - 90, width - 50, height - 90)

        # Métricas con labels alineados
        self._canvas.setFont("Helvetica", 12)
        start_y = height - 130
        line_height = 20
        
    
        # ---- 1. Confusion Matrix ----
        cm = confusion_matrix(y_test, y_pred)
        disp = ConfusionMatrixDisplay(confusion_matrix=cm)
        disp.plot(cmap="Blues", values_format='d')
        plt.title("Confusion Matrix")
        cm_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(cm_file.name)
        plt.close()
        
        # ---- 2. Confidence Histogram ----
        plt.hist(confidences, bins=20, color='skyblue', edgecolor='black')
        plt.title("Prediction Confidence Histogram")
        plt.xlabel("Confidence")
        plt.ylabel("Frequency")
        conf_hist_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(conf_hist_file.name)
        plt.close()
        
        # ---- 3. Accuracy vs. Confidence ----
        sorted_idx = np.argsort(confidences)
        rolling_acc = np.cumsum(y_pred[sorted_idx] == y_test[sorted_idx]) / np.arange(1, len(y_test)+1)
        plt.plot(confidences[sorted_idx], rolling_acc, color="green")
        plt.xlabel("Confidence (sorted)")
        plt.ylabel("Cumulative Accuracy")
        plt.title("Accuracy vs. Confidence")
        acc_conf_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(acc_conf_file.name)
        plt.close()

        metrics = [
            ("Accuracy", accuracy),
            ("Precision", precision),
            ("Recall", recall),
            ("F1 Score", f1),
        ]

        for i, (label, value) in enumerate(metrics):
            y = start_y - i * line_height
            self._canvas.drawString(60, y, f"{label}:")
            self._canvas.drawRightString(200, y, f"{value:.4f}")
            
        img_y_start = start_y - len(metrics)*line_height - 50
        self._canvas.drawImage(cm_file.name, 50, img_y_start - 200, width=500, height=200)
        img_y_start -= 220
        self._canvas.drawImage(conf_hist_file.name, 50, img_y_start - 200, width=500, height=200)
        img_y_start -= 220
        self._canvas.drawImage(acc_conf_file.name, 50, img_y_start - 200, width=500, height=200)

            
        
        self._canvas.setFont("Helvetica-Bold", 10)
        self._canvas.setFillColor(grey)
        self._canvas.drawString(50, 30, "INTI")
        
        self._canvas.setFont("Helvetica-Oblique", 10)
        self._canvas.drawString(50, 50, "This report was automatically generated.")
        
        self.save_report() # Por ahora lo dejo así porque no tengo claro si el paquete UQM nos da y_test y y_pred parciales. De ser así, habría que modificarlo para que se llame al final de la evaluación.
        
        os.unlink(cm_file.name)
        os.unlink(conf_hist_file.name)
        os.unlink(acc_conf_file.name)
        
    def save_report(self):
        self._canvas.save()


    def send_report(self, receiver="guldenjf@gmail.com"):

        message = EmailMessage()

        message.set_content("Test")
        
        with open(f"reports/{self._client_id}.pdf", 'rb') as content_file:
            content = content_file.read()
            message.add_attachment(content, maintype='application', subtype='pdf', filename=f"metrologic_evaluation.pdf")
            
        sender = config_params["email_sender"]
        message["To"] = receiver
        message["From"] = sender
        message["Subject"] = "Metrologic Evaluation Report"

        ctx = ssl.create_default_context()
        
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
            smtp.login(sender, config_params["email_password"])
            smtp.sendmail(sender, receiver, message.as_string())


        
        
        

        