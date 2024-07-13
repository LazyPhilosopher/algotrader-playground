import sys

from PySide6.QtWidgets import QApplication, QLabel, QVBoxLayout, QWidget

from data_control_module.data_control_module import Worker


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.label = QLabel("Starting countdown...")
        layout = QVBoxLayout()
        layout.addWidget(self.label)
        self.setLayout(layout)

        self.worker = Worker(10)  # Create a Worker to count down from 10
        self.worker.update_signal.connect(self.update_label)
        self.worker.start()

    def update_label(self, value):
        self.label.setText(f"Count: {value}")
        if value == 0:
            self.label.setText("Countdown finished!")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
