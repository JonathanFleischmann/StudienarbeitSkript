import tkinter as tk
import threading
from tkinter import messagebox, filedialog
from tkinter import ttk
import sys
from UserInput.import_config import get_config
from UserInput.create_config import create_config
from UserInput.ui_elements import LabelFrame, TextRedirector, Style

def on_submit(callback, stop_thread_var):
    global running_thread
    if running_thread and running_thread.is_alive():
        print("Operation läuft bereits...")
        return
    if validate_entries():
        # Leere die Konsolenausgabe
        console_text.configure(state="normal")
        console_text.delete(1.0, tk.END)
        console_text.configure(state="disabled")

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
    fields = [
        (db_host, "Host"),
        (db_port, "Port"),
        (db_service_name, "Service-Name"),
        (db_username, "Username"),
        (db_password, "Password"),
        (gtfs_path, "GTFS-Pfad"),
    ]

    for field, name in fields:
        if not field.get():
            messagebox.showerror("Fehler", f"{name} darf nicht leer sein.")
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
    global db_host, db_port, db_service_name, db_username, db_password, batch_size, gtfs_path, running_thread, console_text
    running_thread = None

    root = tk.Tk()

    # Variable, um den Thread zu stoppen
    stop_thread_var = tk.BooleanVar(value=False)

    # Hauptfenster erstellen
    root.title("gtfsToOracleDB")
    root.geometry("1587x764")

    # Validierungsfunktion für Ganzzahlen
    vcmd = (root.register(validate_int_input), '%P')

    # Gesamtframe für Eingaben
    input_frame = LabelFrame(root, "Eingabe", tk, ttk).set_columnspan(1).set_padding((4, 1)).build()

    # Labels und Eingabefelder für Datenbankkonfiguration
    db_config_frame = LabelFrame(input_frame, "OracleDB Konfiguration", tk, ttk).build()
    db_host = create_label_entry(db_config_frame, "Host:", 0)
    db_port = create_label_entry(db_config_frame, "Port:", 1, validate_command=vcmd)
    db_service_name = create_label_entry(db_config_frame, "Service-name:", 2)
    db_username = create_label_entry(db_config_frame, "Username:", 3)
    db_password = create_label_entry(db_config_frame, "Password:", 4, show="*")

    # Elemente für Auswahl des GTFS-Ordners
    gtfs_path_frame = LabelFrame(input_frame, "Dateipfad zu GTFS-Dateien", tk, ttk).set_row(1).build()
    gtfs_path = ttk.Entry(gtfs_path_frame)
    gtfs_path.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)
    ttk.Button(gtfs_path_frame, text="Ordner auswählen", command=select_gtfs_path).grid(row=0, column=1, padx=10, pady=10)

    # Eingabe für Batch-Größe
    batch_size_frame = LabelFrame(input_frame, "Batch-Größe", tk, ttk).set_row(2).build()
    batch_size = create_label_entry(batch_size_frame, "Batch-Size:", 2, validate_command=vcmd)

    # Buttons für Verwaltung der Konfigurationsdatei
    ttk.Button(input_frame, text="Konfigurationsdatei erstellen", command=create_config).grid(row=3, column=0, padx=10, pady=10)
    ttk.Button(input_frame, text="Konfigurationsdatei laden", command=load_config_to_fields).grid(row=3, column=1, padx=10, pady=10)

    # Buttons für Starten des Imports und Abbrechen des Imports
    ttk.Button(input_frame, text="Abbrechen", command=lambda: stop_callback(root, stop_thread_var), style="Red.TButton").grid(row=4, column=0, padx=10, pady=10)
    ttk.Button(input_frame, text="Bestätigen", command=lambda: on_submit(callback, stop_thread_var), style="Green.TButton").grid(row=4, column=1, padx=10, pady=10)

    # Frame für Konsolenausgaben
    console_frame = LabelFrame(root, "Konsolenausgaben", tk, ttk).set_padding((4, 10)).set_sticky(tk.NSEW).set_column(2).build()

    # Textfeld für Konsolenausgaben mit Scrollbar
    console_scrollbar = ttk.Scrollbar(console_frame)
    console_scrollbar.grid(row=0, column=1, sticky=tk.NS)
    console_text = tk.Text(console_frame, height=42, width=140, state="disabled", bg="#f7f7f7", yscrollcommand=console_scrollbar.set)
    console_text.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)
    console_scrollbar.config(command=console_text.yview)

    # Checkbox für automatischen Bildlauf
    auto_scroll_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(console_frame, text="Automatisch scrollen", variable=auto_scroll_var).grid(row=1, column=0, padx=10, pady=0, sticky=tk.W)

    # Umleitung der Standardausgabe
    sys.stdout = TextRedirector(console_text, "stdout", auto_scroll_var)
    sys.stderr = TextRedirector(console_text, "stderr", auto_scroll_var)

    # Style für Buttons
    Style(ttk)

    # Verhalten beim Schließen des Fensters definieren
    root.protocol("WM_DELETE_WINDOW", lambda: on_cancel(root, stop_thread_var))

    # Hauptschleife starten
    root.mainloop()


