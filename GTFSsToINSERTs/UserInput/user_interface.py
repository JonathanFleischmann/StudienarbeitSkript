import tkinter as tk
import threading
from tkinter import messagebox, filedialog
from tkinter import ttk
import sys
from PIL import Image, ImageTk
from UserInput.import_config import get_config
from UserInput.create_config import create_config

class TextRedirector(object):
    def __init__(self, widget, tag="stdout", auto_scroll_var=None):
        self.widget = widget
        self.tag = tag
        self.auto_scroll_var = auto_scroll_var
        self.auto_scroll = True
        
        # Configure the font for the tag
        self.widget.tag_configure(self.tag, font=("Tahoma", 10))
        self.widget.tag_configure("bold", font=("Tahoma", 10, "bold"))

        # Bind the <MouseWheel> event to detect when the user scrolls
        self.widget.bind("<MouseWheel>", self.on_mouse_wheel)

        # Load emoji images
        self.emoji_images = {
            "✅": ImageTk.PhotoImage(Image.open("GTFSsToINSERTs/UserInput/images/check_mark.png").resize((19, 19), Image.LANCZOS)),
            "❌": ImageTk.PhotoImage(Image.open("GTFSsToINSERTs/UserInput/images/cross_mark.png").resize((18, 18), Image.LANCZOS)),
            "⚠": ImageTk.PhotoImage(Image.open("GTFSsToINSERTs/UserInput/images/warning.png").resize((16, 16), Image.LANCZOS)),
            "➕": ImageTk.PhotoImage(Image.open("GTFSsToINSERTs/UserInput/images/plus.png").resize((16, 16), Image.LANCZOS)),
            "🔍": ImageTk.PhotoImage(Image.open("GTFSsToINSERTs/UserInput/images/search.png").resize((17, 17), Image.LANCZOS))
        }

    def on_mouse_wheel(self, event):
        # Check if the user has scrolled up
        if self.widget.yview()[1] < 1.0:
            self.auto_scroll = False
        else:
            self.auto_scroll = True

    def write(self, str):
        self.widget.configure(state="normal")
        if '\r' in str:
            self.remove_last_lines_with_content(2)
            str = str.replace('\r', '')

        # Insert text and emojis
        parts = str.split('**')
        for i, part in enumerate(parts):
            if i % 2 == 0:
                for char in part:
                    if char in self.emoji_images:
                        self.widget.image_create("end", image=self.emoji_images[char])
                    else:
                        self.widget.insert("end", char, (self.tag,))
            else:
                self.widget.insert("end", part, ("bold",))

        self.widget.configure(state="disabled")

        # Only scroll to the end if auto_scroll is True and the checkbox is checked
        if self.auto_scroll_var.get() and self.auto_scroll:
            self.widget.see("end")

    def flush(self):
        pass

    def remove_last_lines_with_content(self, num_lines):
        # Get the index of the last line with content
        last_line_index = self.widget.index("end-1c linestart")
        # Remove all following empty lines
        while self.widget.get("end-2c", "end-1c") == "\n":
            self.widget.delete("end-2c", "end-1c")
        # Delete the last num_lines lines with content
        for _ in range(num_lines):
            last_line_index = self.widget.index("end-1c linestart")
            self.widget.delete(last_line_index, "end-1c")

def on_submit(callback, stop_thread_var):
    global running_thread
    if running_thread and running_thread.is_alive():
        print("Operation läuft bereits...")
        return
    if validate_entries():
        # Starte die Callback-Methode in einem eigenen Thread
        thread = threading.Thread(target=lambda: callback(
            db_host.get(), 
            db_port.get(), 
            db_service_name.get(), 
            db_username.get(), 
            db_password.get(), 
            gtfs_path.get(), 
            int(batch_size.get()),
            stop_thread_var
        ), daemon=True)
        thread.start()
        running_thread = thread

def stop_callback(root, stop_thread_var):
    if running_thread and running_thread.is_alive():
        print("Operation abbrechen...")
        stop_thread_var.set(True)
        check_thread(root, stop_thread_var)
        # running_thread.join()  # Warte, bis der Thread beendet ist
        # print("Operation abgebrochen")
        # multi_threading.stop_thread = False  # Setze das Event zurück
        # print("Operation abgebrochen")
    else:
        print("Kein laufender Thread")

def check_thread(root, stop_thread_var):
    """Überprüft, ob der Thread beendet ist, ohne die GUI zu blockieren"""
    if running_thread.is_alive():
        root.after(200, lambda: check_thread(root, stop_thread_var))  # In 100ms erneut prüfen
    else:
        print("Operation abgebrochen")
        stop_thread_var.set(False)

def on_cancel(root, stop_thread_var):
    stop_callback(root, stop_thread_var)
    root.quit()

def validate_entries():
    if not db_host.get():
        messagebox.showerror("Fehler", "Host darf nicht leer sein.")
        return False
    if not db_port.get():
        messagebox.showerror("Fehler", "Port darf nicht leer sein.")
        return False
    if not db_service_name.get():
        messagebox.showerror("Fehler", "Service-name darf nicht leer sein.")
        return False
    if not db_username.get():
        messagebox.showerror("Fehler", "Username darf nicht leer sein.")
        return False
    if not db_password.get():
        messagebox.showerror("Fehler", "Password darf nicht leer sein.")
        return False
    if not gtfs_path.get():
        messagebox.showerror("Fehler", "GTFS-Pfad darf nicht leer sein.")
        return False
    if not batch_size.get().isdigit():
        messagebox.showerror("Fehler", "Batch-Size muss eine Zahl sein.")
        return False
    return True

def load_config_to_fields():
    config = get_config()
    if config:
        db_config = config.get('database', {})
        set_entry_value(db_host, db_config.get('host', ''))
        set_entry_value(db_port, db_config.get('port', ''))
        set_entry_value(db_service_name, db_config.get('service-name', ''))
        set_entry_value(db_username, db_config.get('username', ''))
        set_entry_value(db_password, db_config.get('password', ''))
        set_entry_value(gtfs_path, config.get('input_directory', ''))
        set_entry_value(batch_size, config.get('batch_size', ''))

def select_gtfs_path():
    folder_path = filedialog.askdirectory(title="Ordner auswählen")
    if folder_path:
        set_entry_value(gtfs_path, folder_path)

def set_entry_value(entry, value):
    entry.delete(0, tk.END)
    entry.insert(0, value)

def validate_int_input(P):
    if P.isdigit() or P == "":
        return True
    else:
        return False

def create_label_entry(parent, text, row, show=None, validate_command=None):
    ttk.Label(parent, text=text).grid(row=row, column=0, padx=10, pady=5, sticky=tk.W)
    entry = ttk.Entry(parent, show=show, validate="key", validatecommand=validate_command)
    entry.grid(row=row, column=1, padx=10, pady=5, sticky=tk.EW)
    return entry

def start_user_interface(callback):
    global db_host, db_port, db_service_name, db_username, db_password, batch_size, gtfs_path, running_thread
    running_thread = None

    root = tk.Tk()

    stop_thread_var = tk.BooleanVar(value=False)

    # Hauptfenster erstellen
    root.title("gtfs2OracleDB")
    root.geometry("1587x743")

    # Validierungsfunktion für Ganzzahlen
    vcmd = (root.register(validate_int_input), '%P')

    # Gesamtframe für Eingaben
    input_frame = ttk.LabelFrame(root, text="Eingabe", padding=(4, 1))
    input_frame.grid(row=0, column=0, columnspan=1, padx=10, pady=0, sticky=tk.EW)

    # Frame für Datenbankkonfiguration
    db_config_frame = ttk.LabelFrame(input_frame, text="OracleDB Konfiguration", padding=(10, 10))
    db_config_frame.grid(row=0, column=0, columnspan=2, padx=10, pady=3, sticky=tk.EW)

    # Labels und Eingabefelder für Datenbankkonfiguration
    db_host = create_label_entry(db_config_frame, "Host:", 0)
    db_port = create_label_entry(db_config_frame, "Port:", 1, validate_command=vcmd)
    db_service_name = create_label_entry(db_config_frame, "Service-name:", 2)
    db_username = create_label_entry(db_config_frame, "Username:", 3)
    db_password = create_label_entry(db_config_frame, "Password:", 4, show="*")

    # Frame für GTFS-Pfad
    gtfs_path_frame = ttk.LabelFrame(input_frame, text="Dateipfad zu GTFS-Dateien", padding=(10, 10))
    gtfs_path_frame.grid(row=1, column=0, columnspan=2, padx=10, pady=3, sticky=tk.EW)

    gtfs_path = ttk.Entry(gtfs_path_frame)
    gtfs_path.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)

    select_gtfs_path_button = ttk.Button(gtfs_path_frame, text="Ordner auswählen", command=select_gtfs_path)
    select_gtfs_path_button.grid(row=0, column=1, padx=10, pady=10)

    # Frame für Batch-Size
    batch_size_frame = ttk.LabelFrame(input_frame, text="Batch-Größe", padding=(10, 10))
    batch_size_frame.grid(row=2, column=0, columnspan=2, padx=10, pady=3, sticky=tk.EW)

    ttk.Label(batch_size_frame, text="Batch-Size:").grid(row=2, column=0, padx=10, pady=5, sticky=tk.W)
    batch_size = ttk.Entry(batch_size_frame, validate="key", validatecommand=vcmd)
    batch_size.grid(row=2, column=1, padx=10, pady=5, sticky=tk.EW)

    # Buttons
    create_config_button = ttk.Button(input_frame, text="Konfigurationsdatei erstellen", command=create_config)
    create_config_button.grid(row=3, column=0, padx=10, pady=10)

    select_config_button = ttk.Button(input_frame, text="Konfigurationsdatei laden", command=load_config_to_fields)
    select_config_button.grid(row=3, column=1, padx=10, pady=10)

    cancel_button = ttk.Button(input_frame, text="Abbrechen", command=lambda: stop_callback(root, stop_thread_var), style="Red.TButton")
    cancel_button.grid(row=4, column=0, padx=10, pady=10)

    submit_button = ttk.Button(input_frame, text="Bestätigen", command=lambda: on_submit(callback, stop_thread_var), style="Green.TButton")
    submit_button.grid(row=4, column=1, padx=10, pady=10)

    # Frame für Konsolenausgaben
    console_frame = ttk.LabelFrame(root, text="Konsolenausgaben", padding=(4, 10))
    console_frame.grid(row=0, column=2, columnspan=2, padx=10, pady=10, sticky=tk.NSEW)

    # Scrollbar hinzufügen
    console_scrollbar = ttk.Scrollbar(console_frame)
    console_scrollbar.grid(row=0, column=1, sticky=tk.NS)

    # Textfeld für Konsolenausgaben
    console_text = tk.Text(console_frame, height=42, width=140, state="disabled", bg="#e8f5e9", yscrollcommand=console_scrollbar.set)
    console_text.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)

    console_scrollbar.config(command=console_text.yview)

    # Checkbox für automatischen Bildlauf
    auto_scroll_var = tk.BooleanVar(value=True)
    auto_scroll_checkbox = ttk.Checkbutton(console_frame, text="Automatisch scrollen", variable=auto_scroll_var)
    auto_scroll_checkbox.grid(row=1, column=0, padx=10, pady=5, sticky=tk.W)

    # Umleitung der Standardausgabe
    sys.stdout = TextRedirector(console_text, "stdout", auto_scroll_var)
    sys.stderr = TextRedirector(console_text, "stderr", auto_scroll_var)

    style = get_style()

    # Verhalten beim Schließen des Fensters definieren
    root.protocol("WM_DELETE_WINDOW", lambda: on_cancel(root, stop_thread_var))

    # Hauptschleife starten
    root.mainloop()


def get_style():
    style = ttk.Style()

    # Passe den Standard-Stil für alle Buttons an
    style.configure("TButton",
                    background="#478ced",
                    foreground="black",
                    font=("Arial", 8, "bold"))
    style.map("TButton",
            background=[("active", "#3931b5")])

    # Erstelle einen roten Button-Stil
    style.configure("Red.TButton",
                    background="#e57373",
                    foreground="black",
                    font=("Arial", 10, "bold"),
                    padding=6,
                    relief="flat")
    style.map("Red.TButton",
            background=[("active", "#ef5350")])

    # Erstelle einen grünen Button-Stil
    style.configure("Green.TButton",
                    background="#81c784",
                    foreground="black",
                    font=("Arial", 10, "bold"),
                    padding=6,
                    relief="flat")
    style.map("Green.TButton",
            background=[("active", "#66bb6a")])
    
    # Passe den Standard-Stil für alle Eingabefelder an
    style.configure("TEntry",
                    background="#f5f5f5",
                    font=("Arial", 10),
                    padding=5,
                    relief="flat")
    style.map("TEntry",
            background=[("active", "#e0e0e0")])
    
    return style