

class FakeReportBuilder:
    def __init__(self):
        self.report_built = False
        self.report_sent = False
        self.report_saved = False

    def build_report(self, y_test, probs):
        self.report_built = True
        
    def save_report(self, story):
        self.report_saved = True

    def send_report(self, receiver=""):
        self.report_sent = True
