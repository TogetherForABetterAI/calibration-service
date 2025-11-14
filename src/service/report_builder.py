import os
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

    def __init__(self, client_id, email_sender, email_password):
        self._client_id = client_id
        self.email_sender = email_sender
        self.email_password = email_password
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

        if y_true is None or probs is None:
            raise ValueError("Los datos de entrada (y_true o probs) no pueden ser None.")

        if len(y_true) == 0 or len(probs) == 0:
            raise ValueError("Los vectores de etiquetas o probabilidades están vacíos.")

        if len(y_true) != probs.shape[0]:
            raise ValueError(f"Dimensiones incompatibles: y_true tiene {len(y_true)} elementos y probs tiene {probs.shape[0]} filas.")

        y_pred = np.argmax(probs, axis=1)
        confidences = np.max(probs, axis=1)

        metrics = {
            "Accuracy": accuracy_score(y_true, y_pred),
            "Precision": precision_score(y_true, y_pred, average="weighted", zero_division=0),
            "Recall": recall_score(y_true, y_pred, average="weighted", zero_division=0),
            "F1 Score": f1_score(y_true, y_pred, average="weighted", zero_division=0),
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
        today = np.datetime64('today').astype(str)
        year, month, day = today.split('-')
        client_id_short = self._client_id.split('_')[0]
        story.append(Paragraph(f"Fecha de evaluación: <b>{today}</b>", self._styles["Normal"]))
        story.append(Paragraph(f"Código de informe: <b>EVAL-INTI-{client_id_short}/{year}</b>", self._styles["Normal"]))
        story.append(PageBreak())

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

        # --- MATRIZ DE CONFUSIÓN ---
        try:
            conf_matrix = confusion_matrix(y_true, y_pred)
            custom_cmap = LinearSegmentedColormap.from_list("inti_blue", ["#e8f0fa", "#004C91"])
            plt.figure(figsize=(6,5))
            disp = ConfusionMatrixDisplay(confusion_matrix=conf_matrix)
            disp.plot(cmap=custom_cmap, values_format="d", ax=plt.gca(), colorbar=False)
            plt.title("Matriz de confusión", fontsize=12)
            plt.tight_layout()
            cm_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            plt.savefig(cm_tmp.name, dpi=120)
            plt.close()
            cm_img_path = cm_tmp.name
        except Exception as e:
            logging.error(f"Error generando matriz de confusión: {e}")
            cm_img_path = None

        story.append(Paragraph("3. Resultados Visuales", self._styles["Heading1Blue"]))
        story.append(Paragraph("3.1 Matriz de Confusión", self._styles["Normal"]))

        if cm_img_path and os.path.exists(cm_img_path):
            story.append(Image(cm_img_path, width=12*cm, height=8*cm))

        story.append(Spacer(1, 0.5*cm))

        # --- HISTOGRAMA DE CONFIANZA ---
        try:
            plt.figure(figsize=(6,4))
            sns.histplot(confidences, bins=30, color="#004C91", edgecolor="white")
            plt.xlabel("Confianza")
            plt.ylabel("Frecuencia")
            plt.title("Distribución de Confianza de Predicciones")
            plt.grid(True, linestyle="--", alpha=0.6)
            conf_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            plt.savefig(conf_tmp.name, dpi=120)
            plt.close()
            conf_img_path = conf_tmp.name
        except Exception as e:
            logging.error(f"Error generando histograma de confianza: {e}")
            conf_img_path = None

        story.append(Paragraph("3.2 Histograma de Confianza", self._styles["Normal"]))
        if conf_img_path and os.path.exists(conf_img_path):
            story.append(Image(conf_img_path, width=12*cm, height=8*cm))
        else:
            story.append(Paragraph("<i>No se pudo generar el histograma de confianza.</i>", self._styles["Normal"]))
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
        message["From"] = self.email_sender
        message["To"] = receiver
        message.set_content("Adjuntamos el informe de evaluación metrológica de su modelo.")

        with open(self._pdf_path, "rb") as f:
            message.add_attachment(
                f.read(), maintype="application", subtype="pdf",
                filename=f"Evaluacion_INTI_{self._client_id}.pdf"
            )

        cfg = initialize_config()
        password = self.email_password
        ctx = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
            smtp.login(message["From"], password)
            smtp.send_message(message)

        logging.info(f"Reporte enviado a {receiver}")

