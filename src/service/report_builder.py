from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import logging
from reportlab.lib.colors import grey
import base64
from email.message import EmailMessage
from lib.config import config_params
import smtplib
import ssl


class ReportBuilder:
    def __init__(self, client_id):
        self._probs = {} # Dictionary with {[] prob_outputs : int label}
        self._client_id = client_id
        self._canvas = canvas.Canvas(f"reports/{self._client_id}.pdf", pagesize=A4)

    def build_report(self, y_test, y_pred):
        logging.info(f"Generating report for client {self._client_id}")
        # accuracy = accuracy_score(y_test, y_pred)
        # precision = precision_score(y_test, y_pred, average='weighted')
        # recall = recall_score(y_test, y_pred, average='weighted')
        # f1 = f1_score(y_test, y_pred, average='weighted')
        accuracy = 0.5
        precision = 0.5
        recall = 0.5
        f1 = 0.5
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
            
        
        self._canvas.setFont("Helvetica-Bold", 10)
        self._canvas.setFillColor(grey)
        self._canvas.drawString(50, 30, "INTI")
        
        self._canvas.setFont("Helvetica-Oblique", 10)
        self._canvas.drawString(50, 50, "This report was automatically generated.")
        self.save_report() # Por ahora lo dejo así porque no tengo claro si el paquete UQM nos da y_test y y_pred parciales. De ser así, habría que modificarlo para que se llame al final de la evaluación.
        
        
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


        
        
        

        