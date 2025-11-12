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
        #     fontName="Times-Roman",import os
import tempfile
import ssl
import smtplib
import logging
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, ConfusionMatrixDisplay
)
from email.message import EmailMessage
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import inch, cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from lib.config import initialize_config


class ReportBuilder:
    """
    Genera un reporte PDF institucional de evaluación metrológica,
    con estructura y estilo compatibles con los informes del INTI.
    """

    def __init__(self, client_id):
        self._client_id = client_id
        os.makedirs("reports", exist_ok=True)
        self._pdf_path = f"reports/{self._client_id}.pdf"

        # Inicializar documento base
        self._doc = SimpleDocTemplate(
            self._pdf_path,
            pagesize=A4,
            title="Informe de Evaluación Metrológica",
            author="INTI - Metrología de Datos",
            leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm,
        )

        # Estilos generales
        self._styles = getSampleStyleSheet()
        self._styles.add(ParagraphStyle(name="Heading1Blue",
                                        parent=self._styles["Heading1"],
                                        textColor=colors.HexColor("#004C91")))
        self._styles.add(ParagraphStyle(name="NormalJustified",
                                        parent=self._styles["Normal"],
                                        alignment=4))
        self._styles.add(ParagraphStyle(name="CenterTitle",
                                        parent=self._styles["Title"],
                                        alignment=1, textColor=colors.HexColor("#004C91")))

    def build_report(self, y_true, probs):
        """
        Construye el PDF de evaluación completo a partir de las predicciones y etiquetas reales.
        """
        logging.info(f"Generando reporte institucional para cliente {self._client_id}")

        y_pred = np.argmax(probs, axis=1)
        confidences = np.max(probs, axis=1)

        # Métricas globales
        metrics = {
            "Accuracy": accuracy_score(y_true, y_pred),
            "Precision": precision_score(y_true, y_pred, average="weighted"),
            "Recall": recall_score(y_true, y_pred, average="weighted"),
            "F1 Score": f1_score(y_true, y_pred, average="weighted"),
        }

        story = []

        # --- PORTADA ---
        story.append(Spacer(1, 1*cm))
        logo_path = "reports/inti_logo.png"
        if os.path.exists(logo_path):
            logo = Image(logo_path, width=3*inch, height=3*inch)
            logo.hAlign = "CENTER"
            story.append(logo)
            story.append(Spacer(1, 0.5*cm))

        story.append(Paragraph("INSTITUTO NACIONAL DE TECNOLOGÍA INDUSTRIAL", self._styles["Heading1Blue"]))
        story.append(Spacer(1, 0.2*cm))
        story.append(Paragraph("Informe de Evaluación Metrológica de Modelo de Inteligencia Artificial", self._styles["CenterTitle"]))
        story.append(Spacer(1, 1*cm))
        story.append(Paragraph(f"Cliente: <b>{self._client_id}</b>", self._styles["Normal"]))
        story.append(Paragraph("Fecha de evaluación: <b>2025-11-11</b>", self._styles["Normal"]))
        story.append(Paragraph("Código de informe: <b>EVAL-INTI-XXX/2025</b>", self._styles["Normal"]))
        story.append(PageBreak())

        # --- RESUMEN ---
        story.append(Paragraph("1. Resumen Ejecutivo", self._styles["Heading1Blue"]))
        story.append(Spacer(1, 0.2*cm))
        summary_text = """
        Este informe presenta los resultados de la evaluación metrológica del modelo provisto.
        Se analiza la calibración de las salidas de probabilidad (softmax), el nivel de desempeño global
        y la distribución de las predicciones.
        """
        story.append(Paragraph(summary_text, self._styles["NormalJustified"]))
        story.append(Spacer(1, 0.5*cm))

        # --- MÉTRICAS ---
        story.append(Paragraph("2. Métricas de Evaluación", self._styles["Heading1Blue"]))
        table_data = [["Métrica", "Valor"]] + [[k, f"{v:.4f}"] for k, v in metrics.items()]
        table = Table(table_data, colWidths=[6*cm, 3*cm])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#004C91")),
            ("TEXTCOLOR", (0,0), (-1,0), colors.white),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTNAME", (0,0), (-1,-1), "Helvetica")
        ]))
        story.append(table)
        story.append(Spacer(1, 0.5*cm))

        # --- CONFUSION MATRIX ---
        cm = confusion_matrix(y_true, y_pred)
        custom_cmap = LinearSegmentedColormap.from_list("inti_blue", ["#e8f0fa", "#004C91"])
        plt.figure(figsize=(6,5))
        disp = ConfusionMatrixDisplay(confusion_matrix=cm)
        disp.plot(cmap=custom_cmap, values_format="d", ax=plt.gca(), colorbar=False)
        plt.title("Matriz de confusión", fontsize=12)
        plt.tight_layout()
        cm_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(cm_tmp.name, dpi=120)
        plt.close()

        story.append(Paragraph("3. Resultados Visuales", self._styles["Heading1Blue"]))
        story.append(Paragraph("3.1 Matriz de Confusión", self._styles["Normal"]))
        story.append(Image(cm_tmp.name, width=12*cm, height=8*cm))
        story.append(Spacer(1, 0.5*cm))

        # --- HISTOGRAMA DE CONFIANZA ---
        plt.figure(figsize=(6,4))
        sns.histplot(confidences, bins=30, color="#004C91", edgecolor="white")
        plt.xlabel("Confianza")
        plt.ylabel("Frecuencia")
        plt.title("Distribución de Confianza de Predicciones")
        plt.grid(True, linestyle="--", alpha=0.6)
        conf_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(conf_tmp.name, dpi=120)
        plt.close()

        story.append(Paragraph("3.2 Histograma de Confianza", self._styles["Normal"]))
        story.append(Image(conf_tmp.name, width=12*cm, height=8*cm))
        story.append(Spacer(1, 0.5*cm))

        # --- CONCLUSIONES ---
        story.append(Paragraph("4. Conclusiones", self._styles["Heading1Blue"]))
        conclusion_text = """
        El modelo presenta un desempeño general satisfactorio y una distribución de confianza consistente.
        Se recomienda monitorear periódicamente la calibración para asegurar la estabilidad metrológica
        en futuros despliegues.
        """
        story.append(Paragraph(conclusion_text, self._styles["NormalJustified"]))
        story.append(Spacer(1, 1*cm))

        # --- FIRMA ---
        story.append(Paragraph(
            "<b>INTI – Instituto Nacional de Tecnología Industrial</b><br/>"
            "Dirección Nacional de Metrología Científica y Aplicada<br/>"
            "División Metrología de Datos y Modelos<br/><br/>"
            "Firma digital:<br/><br/><b>Ing. Juan Pérez</b><br/>"
            "Responsable Técnico – Ensayos de Modelos de IA",
            self._styles["NormalJustified"]
        ))

        # --- BUILD FINAL PDF ---
        self._doc.build(story, onFirstPage=self._add_footer, onLaterPages=self._add_footer)

        # Cleanup
        os.unlink(cm_tmp.name)
        os.unlink(conf_tmp.name)
        logging.info(f"Reporte generado en {self._pdf_path}")

    def _add_footer(self, canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.grey)
        canvas.drawString(2*cm, 1.5*cm,
                          "Este informe fue generado automáticamente por el sistema de evaluación metrológica – INTI")
        canvas.restoreState()

    def send_report(self, receiver):
        """
        Envía el reporte por correo electrónico utilizando SMTP seguro.
        """
        message = EmailMessage()
        message["Subject"] = "Informe de Evaluación Metrológica – INTI"
        message["From"] = os.getenv("EMAIL_SENDER", "noreply@inti.gob.ar")
        message["To"] = receiver
        message.set_content("Adjuntamos el informe de evaluación metrológica de su modelo.")

        with open(self._pdf_path, "rb") as f:
            message.add_attachment(
                f.read(), maintype="application", subtype="pdf",
                filename=f"Evaluacion_INTI_{self._client_id}.pdf"
            )

        cfg = initialize_config()
        password = os.getenv("EMAIL_PASSWORD", "")
        ctx = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
            smtp.login(message["From"], password)
            smtp.send_message(message)

        logging.info(f"Reporte enviado a {receiver}")

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
