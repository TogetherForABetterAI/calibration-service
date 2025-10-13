import os
from matplotlib.colors import LinearSegmentedColormap
from reportlab.lib.pagesizes import A4
import logging
from email.message import EmailMessage
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from lib.config import initialize_config
import smtplib
import ssl
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import tempfile
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    Table,
    TableStyle,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import tempfile
import matplotlib.pyplot as plt
import seaborn as sns


class ReportBuilder:
    def __init__(self, client_id):
        self._probs = {}  # Dictionary with {[] prob_outputs : int label}
        self._client_id = client_id
        # Ensure the reports directory exists
        os.makedirs("reports", exist_ok=True)
        self._doc = SimpleDocTemplate(
            f"reports/{self._client_id}.pdf",
            pagesize=A4,
            title="Conformal Predictor Report",
            author="Metrologic Team",
        )

    def build_report(self, y_test, probs):
        logging.info(f"Generating report for client {self._client_id}")

        y_pred = np.argmax(probs, axis=1)
        confidences = np.max(probs, axis=1)

        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average="weighted")
        recall = recall_score(y_test, y_pred, average="weighted")
        f1 = f1_score(y_test, y_pred, average="weighted")

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "Title",
            parent=styles["Title"],
            fontName="Times-Roman",
            fontSize=20,
            leading=24,
            alignment=1,
        )
        subtitle_style = ParagraphStyle(
            "Subtitle",
            parent=styles["Heading2"],
            fontName="Times-Roman",
            fontSize=14,
            leading=18,
            alignment=1,
        )
        # normal_style = ParagraphStyle(
        #     "Normal",
        #     parent=styles["Normal"],
        #     fontName="Times-Roman",
        #     fontSize=12,
        #     leading=14,
        #     alignment=1,
        # )

        story = []

        story.append(Spacer(1, 30))
        logo_path = "reports/inti_logo.png"
        logo = Image(logo_path, width=2 * inch, height=2 * inch)
        logo.hAlign = "CENTER"
        story.append(logo)
        story.append(Spacer(1, 20))

        story.append(Paragraph("Conformal Predictor Report", title_style))
        story.append(Spacer(1, 12))

        story.append(Paragraph(f"User {self._client_id}", subtitle_style))
        story.append(Spacer(1, 20))

        story.append(Paragraph("Metrics", subtitle_style))
        story.append(Spacer(1, 6))

        # Métricas en tabla
        metrics = [
            ["Accuracy", f"{accuracy:.4f}"],
            ["Precision", f"{precision:.4f}"],
            ["Recall", f"{recall:.4f}"],
            ["F1 Score", f"{f1:.4f}"],
        ]
        t = Table(metrics, colWidths=[2 * inch, 2 * inch])
        t.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("FONTNAME", (0, 0), (-1, -1), "Times-Roman"),
                    ("FONTSIZE", (0, 0), (-1, -1), 10),
                ]
            )
        )
        story.append(t)
        story.append(Spacer(1, 24))

        # ---- 1. Confusion Matrix ----
        cm = confusion_matrix(y_test, y_pred)
        plt.figure(figsize=(6, 8))
        sns.set_style("white")
        custom_cmap = LinearSegmentedColormap.from_list(
            "custom_blue", ["#ffffff", "#34495e"]
        )
        disp = ConfusionMatrixDisplay(confusion_matrix=cm)
        disp.plot(cmap=custom_cmap, values_format="d", ax=plt.gca(), colorbar=True)
        plt.xlabel("Predicted Label", fontsize=12)
        plt.ylabel("True Label", fontsize=12)
        plt.tight_layout()
        cm_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(cm_file.name)
        plt.close()

        # ---- 2. Confidence Histogram ----
        plt.figure(figsize=(6, 8))
        plt.hist(confidences, bins=30, color="#2c3e50", edgecolor="white", alpha=0.9)
        plt.xlabel("Confidence", fontsize=12, color="#34495e")
        plt.ylabel("Frequency", fontsize=12, color="#34495e")
        plt.gca().set_facecolor("#ecf0f1")
        plt.grid(axis="y", linestyle="--", alpha=0.7, color="#bdc3c7")
        plt.tight_layout()
        conf_hist_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(conf_hist_file.name, dpi=100)
        plt.close()

        # Agregar imágenes
        story.append(Paragraph("Confusion Matrix", subtitle_style))
        story.append(Spacer(1, 6))
        story.append(Image(cm_file.name, width=5 * inch, height=3 * inch))
        story.append(Spacer(1, 24))

        # story.append(PageBreak())
        story.append(Paragraph("Prediction Confidence Histogram", subtitle_style))
        story.append(Spacer(1, 6))
        story.append(Image(conf_hist_file.name, width=5 * inch, height=3 * inch))
        story.append(Spacer(1, 24))

        # Construir el PDF
        self.save_report(
            story
        )  # Por ahora lo dejo así porque no tengo claro si el paquete UQM nos da y_test y y_pred parciales. De ser así, habría que modificarlo para que se llame al final de la evaluación.

        os.unlink(cm_file.name)
        os.unlink(conf_hist_file.name)

    def save_report(self, story):
        self._doc.build(story, onFirstPage=add_footer, onLaterPages=add_footer)

    def send_report(self, receiver="guldenjf@gmail.com"):

        message = EmailMessage()

        message.set_content("Test")

        with open(f"reports/{self._client_id}.pdf", "rb") as content_file:
            content = content_file.read()
            message.add_attachment(
                content,
                maintype="application",
                subtype="pdf",
                filename=f"metrologic_evaluation.pdf",
            )

        config = initialize_config()
        sender = os.getenv("EMAIL_SENDER", "someone@example.com")
        password = os.getenv("EMAIL_PASSWORD", "abcdefghijklmnop")
        message["To"] = receiver
        message["From"] = sender
        message["Subject"] = "Metrologic Evaluation Report"

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
            smtp.login(sender, password)
            smtp.sendmail(sender, receiver, message.as_string())


def add_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Times-Roman", 8)
    canvas.drawString(72, 20, "This report was automatically generated.")
    canvas.restoreState()
