from datetime import datetime
from email.message import EmailMessage
import logging
import os
import smtplib
import ssl
import numpy as np
import matplotlib.pyplot as plt
import tempfile
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import MaxNLocator
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm, inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT

from src.lib.config import INTI_LOGO_PATH, REPORTS_DIR, SIGNATURE_PATH


class ReportBuilder:
    """
    Genera un reporte PDF institucional de evaluación metrológica,
    con estructura y estilo compatibles con los informes del INTI.
    """

    def __init__(self, user_id, email_sender, email_password, output_dir=REPORTS_DIR):
        self._user_id = user_id
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self._pdf_path = f"{self.output_dir}/{self._user_id}.pdf"
        self.email_sender = email_sender
        self.email_password = email_password
        self._styles = self._create_styles()
        self._doc = SimpleDocTemplate(
            self._pdf_path,
            pagesize=A4,
            title="Informe de Evaluación de Incertidumbre",
            author="INTI - Departamento de Inteligencia Artificial",
            leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm,
        )
        
        plt.switch_backend('Agg')

    def _create_styles(self):
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name="Heading1Blue",
            parent=styles['Heading1'],
            fontName="Helvetica-Bold",
            fontSize=16,
            textColor=colors.HexColor("#004C91"),
            spaceAfter=12,
            alignment=TA_LEFT
        ))
        
        styles.add(ParagraphStyle(
            name="CenterTitle",
            parent=styles['Title'],
            fontName="Helvetica-Bold",
            fontSize=20,
            textColor=colors.black,
            alignment=TA_CENTER,
            spaceAfter=20
        ))
        styles.add(ParagraphStyle(
            name="NormalJustified",
            parent=styles['Normal'],
            fontName="Helvetica",
            fontSize=11,
            leading=14,
            alignment=TA_JUSTIFY
        ))
        return styles

    def _add_footer(self, canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 9)
        canvas.drawString(2 * cm, 1.5 * cm, "U-TraCE Calibration Report")
        canvas.drawRightString(19 * cm, 1.5 * cm, f"Página {doc.page}")
        canvas.setStrokeColor(colors.HexColor("#004C91"))
        canvas.setLineWidth(1)
        canvas.line(2*cm, 2*cm, 19*cm, 2*cm)
        canvas.restoreState()

    def generate_report(self, calibration_results, filename="utrace_report.pdf"):
        """
        Genera un PDF profesional con la carátula, métricas y 3 visualizaciones clave.
        """
        story = []
        
        metrics = calibration_results.get("metrics", {})
        history = calibration_results.get("history", {})
        raw_data = calibration_results.get("raw_data", {})

        # =========================================================================
        # 1. PORTADA
        # =========================================================================
        story.append(Spacer(1, 1*cm))
        logo_path = INTI_LOGO_PATH
        if os.path.exists(logo_path):
            logo = Image(logo_path, width=3*inch, height=3*inch)
            logo.hAlign = "CENTER"
            story.append(logo)
            story.append(Spacer(1, 0.5*cm))
        
        story.append(Spacer(1, 1.5*cm))
        story.append(Paragraph("INSTITUTO NACIONAL DE TECNOLOGÍA INDUSTRIAL", self._styles["Heading1Blue"]))
        story.append(Spacer(1, 0.2*cm))
        story.append(Paragraph("Informe de Evaluación de Incertidumbre de Inteligencia Artificial", self._styles["CenterTitle"]))
        story.append(Spacer(1, 1.5*cm))
        
        # Datos del cliente
        story.append(Paragraph(f"<b>ID Cliente:</b> {self._user_id}", self._styles["Normal"]))
        story.append(Paragraph(f"<b>Fecha de emisión:</b> {datetime.now().strftime('%Y-%m-%d')}", self._styles["Normal"]))
        story.append(Paragraph(f"<b>Código de reporte:</b> UTC-{datetime.now().strftime('%y%m%d')}-001", self._styles["Normal"]))
        story.append(PageBreak())

        # =========================================================================
        # 2. RESUMEN DE MÉTRICAS (Tabla)
        # =========================================================================
        story.append(Paragraph("2. Resumen Ejecutivo", self._styles["Heading1Blue"]))
        story.append(Paragraph(
            "A continuación se detallan las métricas obtenidas tras el proceso de calibración conformal (U-TraCE). "
            "Estas métricas garantizan que el riesgo operativo del modelo está acotado dentro de los parámetros establecidos.",
            self._styles["NormalJustified"]
        ))
        story.append(Spacer(1, 0.5*cm))

        table_data = [["Métrica de Desempeño", "Valor Obtenido"]]
        for k, v in metrics.items():
            val_str = f"{v:.4f}" if isinstance(v, float) else str(v)
            if "Coverage" in k: val_str = f"{v:.2%}"
            table_data.append([k, val_str])

        table = Table(table_data, colWidths=[10*cm, 5*cm])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#004C91")),
            ("TEXTCOLOR", (0,0), (-1,0), colors.white),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,0), (-1,0), 12),
            ("BOTTOMPADDING", (0,0), (-1,0), 10),
            ("GRID", (0,0), (-1,-1), 0.5, colors.lightgrey),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.whitesmoke, colors.white]),
        ]))
        story.append(table)
        story.append(Spacer(1, 1*cm))

        # =========================================================================
        # 3. ANÁLISIS DEL MODELO BASE (Visuales)
        # =========================================================================
        story.append(Paragraph("3. Comportamiento del Modelo Base", self._styles["Heading1Blue"]))
        story.append(Paragraph(
            "Análisis de las predicciones 'puntuales' (hard predictions) antes de aplicar conjuntos de predicción conformal.",
            self._styles["NormalJustified"]
        ))
        story.append(Spacer(1, 0.5*cm))

        # --- Fila de gráficos 1: Confusión + Confianza ---
        row_images = []
        
        # A. Matriz de Confusión
        try:
            if len(raw_data.get('y_true', [])) > 0:
                plt.figure(figsize=(5, 4))
                cm_mat = confusion_matrix(raw_data['y_true'], raw_data['y_pred'])
                # Mapa de color azul institucional
                cmap_inti = LinearSegmentedColormap.from_list("inti", ["#ffffff", "#004C91"])
                disp = ConfusionMatrixDisplay(confusion_matrix=cm_mat)
                disp.plot(cmap=cmap_inti, values_format="d", ax=plt.gca(), colorbar=False)
                plt.title("Matriz de Confusión")
                plt.ylabel("Etiqueta Real")
                plt.xlabel("Predicción")
                plt.tight_layout()
                
                tmp_cm = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
                plt.savefig(tmp_cm.name, dpi=150)
                plt.close()
                row_images.append(Image(tmp_cm.name, width=8*cm, height=6.4*cm))
        except Exception as e:
            logging.error(f"Error CM: {e}")

        # B. Histograma de Confianza
        try:
            if len(raw_data.get('confidences', [])) > 0:
                plt.figure(figsize=(5, 4))
                sns.histplot(raw_data['confidences'], bins=20, color="#004C91", kde=True)
                plt.axvline(np.mean(raw_data['confidences']), color='red', linestyle='--', label='Media')
                plt.title("Distribución de Confianza")
                plt.xlabel("Probabilidad")
                plt.ylabel("Frecuencia")
                plt.legend()
                plt.tight_layout()
                
                tmp_hist = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
                plt.savefig(tmp_hist.name, dpi=150)
                plt.close()
                row_images.append(Image(tmp_hist.name, width=8*cm, height=6.4*cm))
        except Exception as e:
            logging.error(f"Error Hist: {e}")

        if row_images:
            if len(row_images) == 2:
                table_imgs = Table([row_images], colWidths=[8.5*cm, 8.5*cm])
                table_imgs.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER')]))
                story.append(table_imgs)
            else:
                story.append(row_images[0])
        else:
            story.append(Paragraph("<i>No hay suficientes datos para generar gráficos base.</i>", self._styles["Normal"]))

        story.append(Spacer(1, 0.5*cm))

        # =========================================================================
        # 4. DASHBOARD DE CALIBRACIÓN U-TRACE
        # =========================================================================
        story.append(Paragraph("4. Dinámica de Calibración (Dashboard)", self._styles["Heading1Blue"]))
        story.append(Paragraph(
            "Evolución de los parámetros internos del sistema U-TraCE a lo largo de las etapas de "
            "estimación de incertidumbre y construcción de conjuntos.",
            self._styles["NormalJustified"]
        ))
        story.append(Spacer(1, 0.5*cm))

        dash_path = self._generate_dashboard_plot(history, metrics)
        if dash_path:
            story.append(Image(dash_path, width=17*cm, height=12*cm))
        
        # =========================================================================
        # 5. CONCLUSIONES Y FIRMA
        # =========================================================================
        story.append(PageBreak())
        story.append(Paragraph("5. Dictamen Técnico", self._styles["Heading1Blue"]))
        
        acc = metrics.get("Accuracy", 0)
        cov = metrics.get("Empirical Coverage", 0)
        
        verdict = "SATISFACTORIO" if (cov >= 0.90) else "REQUIERE REVISIÓN" 
        color_verdict = "green" if verdict == "SATISFACTORIO" else "red"

        conclusion = f"""
        El modelo evaluado ha completado el proceso de calibración U-TraCE. 
        Con una exactitud base del <b>{acc:.2%}</b>, el sistema de predicción conformal logró una cobertura empírica 
        del <b>{cov:.2%}</b>. <br/><br/>
        El estado final de la evaluación se considera: <font color='{color_verdict}'><b>{verdict}</b></font>.
        """
        story.append(Paragraph(conclusion, self._styles["NormalJustified"]))
        
        story.append(Spacer(1, 3*cm))

        signature_path = SIGNATURE_PATH

        if os.path.exists(signature_path):
            sig_img = Image(signature_path, width=4.5*cm, height=2.5*cm)
            sig_img.hAlign = 'LEFT'
            story.append(sig_img)
            story.append(Spacer(1, -0.3*cm)) 
        else:
            story.append(Spacer(1, 2*cm))
        
        # Bloque de firma
        story.append(Paragraph(
            "_________________________________________<br/>"
            "<b>Ing. Responsable</b><br/>"
            "Departamento de Inteligencia Artificial - INTI",
            self._styles["Normal"]
        ))

        try:
            self._doc.build(story, onFirstPage=self._add_footer, onLaterPages=self._add_footer)
            logging.info(f"Reporte generado: {self._pdf_path}")
        except Exception as e:
            logging.error(f"Error building PDF: {e}")
        finally:
            pass

    def _generate_dashboard_plot(self, history, metrics):
        """Genera el grid 2x2 con escalas dinámicas."""
        try:
            alphas = np.array(history.get('alphas', []))
            uncert = np.array(history.get('uncertainty', []))
            coverages = np.array(history.get('batch_coverage', []))
            setsizes = np.array(history.get('batch_setsizes', []))
            
            final_alpha = metrics.get('Alpha', 0.05)
            target_cov = 1.0 - final_alpha

            plt.style.use('ggplot')
            fig, axes = plt.subplots(2, 2, figsize=(10, 7))
            
            # --- 1. Alpha (Riesgo) ---
            ax1 = axes[0, 0]
            if len(alphas) > 0:
                ax1.plot(alphas, color='tab:blue', label='Alpha')
                ax1.axhline(final_alpha, color='r', linestyle='--', label='Final')
                
                ax1.set_ylim(bottom=0) 
                ax1.margins(y=0.1) 
                
                ax1.set_title("Evolución de Alpha")
                ax1.legend()
            
            # --- 2. Incertidumbre ---
            ax2 = axes[0, 1]
            if len(uncert) > 0:
                ax2.plot(uncert, color='tab:orange')
                
                # Igual aquí: piso en 0, techo dinámico
                ax2.set_ylim(bottom=0)
                ax2.margins(y=0.1)
                
                ax2.set_title("Incertidumbre Detectada (U)")
            
            # --- 3. Cobertura ---
            ax3 = axes[1, 0]
            if len(coverages) > 0:
                ax3.plot(np.arange(len(coverages)), coverages, color='tab:green', marker='o', 
                        markersize=4, label='Real')
                ax3.axhline(target_cov, color='r', linestyle='--', 
                            linewidth=2, label='Target')
                
                ax3.fill_between(np.arange(len(coverages)), coverages, alpha=0.2, color='tab:green')
                
                # CAMBIO CLAVE: Quitamos set_ylim(0, 1). 
                # Dejamos que matplotlib ajuste, pero nos aseguramos que incluya el target
                # Calculamos min y max considerando también la línea de target
                data_min = min(np.min(coverages), target_cov)
                data_max = max(np.max(coverages), target_cov)
                
                # Damos un "colchón" (padding) del 20% arriba y abajo de la variación
                span = (data_max - data_min) if (data_max - data_min) > 0 else 0.1
                ax3.set_ylim(data_min - span*0.2, data_max + span*0.2)
                
                ax3.set_title("Cobertura por Batch")
                ax3.legend(loc='best') # 'best' busca el lugar vacío automáticamente

            # --- 4. Set Sizes ---
            ax4 = axes[1, 1]
            if len(setsizes) > 0:
                markerline, stemlines, baseline = ax4.stem(np.arange(len(setsizes)), setsizes, basefmt=" ")
                
                plt.setp(stemlines, 'color', 'tab:purple', 'linewidth', 1.5)
                plt.setp(markerline, 'color', 'tab:purple', 'marker', 'o')

                ax4.yaxis.set_major_locator(MaxNLocator(integer=True))
                
                avg_size = np.mean(setsizes)
                ax4.axhline(avg_size, color='gray', linestyle=':', alpha=0.7, label=f'Prom: {avg_size:.1f}')
                ax4.set_ylim(bottom=0)
                ax4.set_ylim(top=np.max(setsizes) * 1.15) 
                
                ax4.set_title("Tamaño de Sets (Max)")
                ax4.legend()

            plt.tight_layout()
            
            tmp_dash = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            plt.savefig(tmp_dash.name, dpi=120)
            plt.close()
            return tmp_dash.name

        except Exception as e:
            logging.error(f"Error generando dashboard: {e}")
            return None

    def send_report(self, receiver):
        """
        Envía el reporte por correo electrónico utilizando SMTP seguro.
        """
        message = EmailMessage()
        message["Subject"] = "Informe de Evaluación de Incertidumbre – INTI"
        message["From"] = self.email_sender
        message["To"] = receiver
        html = f"""
<html>
    <body style="font-family: Arial, Helvetica, sans-serif; font-size: 14px; color: #333; line-height: 1.6; margin: 0; padding: 0;">
        
        <p>Estimado/a,</p>

        <p>
        Adjuntamos el <strong>Reporte de Evaluación Metrológica</strong> correspondiente a su modelo.
        El documento incluye una síntesis de los resultados estadísticos obtenidos, junto con el análisis 
        de calibración probabilística y cuantificación de incertidumbre realizado durante la evaluación.
        </p>

        <p>
        <em>Este es un correo generado automáticamente. Por favor, no responda a este mensaje.</em>
        </p>

        <p>
        Ante cualquier consulta, comuníquese a través de los canales habituales de soporte.
        </p>

        <br>

        <p>Saludos cordiales,<br>
        <strong>Departamento de Inteligencia Artificial – INTI</strong></p>

    </body>
</html>
        """
        message.add_alternative(html, subtype="html")
        with open(self._pdf_path, "rb") as f:
            message.add_attachment(
                f.read(), maintype="application", subtype="pdf",
                filename=f"Evaluacion_INTI_{self._user_id}.pdf"
            )

        ctx = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
            smtp.login(message["From"], self.email_password)
            smtp.send_message(message)

        logging.info(f"Reporte enviado a {receiver}")


