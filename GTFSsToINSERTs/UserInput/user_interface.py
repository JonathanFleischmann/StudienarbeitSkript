import tkinter as tk
import threading
from tkinter import filedialog
from tkinter import ttk
import sys
from enum import Enum
from UserInput.import_config import get_config
from UserInput.create_config import create_config
from UserInput.ui_elements import LabelFrame, TextRedirector, Style, set_entry_value
from UserInput.validation import validate_entries, validate_int_input
from UserInput.thread_control import stop_thread


class ModeEnum(Enum):
    CREATE_TABLES = 1
    INSERT_GTFS = 2
    RELATIONAL_TO_GRAPH = 3
    PERFORMANCE_ANALYSIS = 4



def on_submit(stop_thread_var):
    global running_thread, mode
    if running_thread and running_thread.is_alive():
        print("Es ist bereits eine Operation aktiv...")
        return
    if mode == ModeEnum.CREATE_TABLES:
        thread = threading.Thread(target=lambda: submit_create_tables(stop_thread_var), daemon=True)
        thread.start()
        running_thread = thread
    elif mode == ModeEnum.INSERT_GTFS:
        thread = threading.Thread(target=lambda: submit_insert_gtfs(stop_thread_var), daemon=True)
        thread.start()
        running_thread = thread

def submit_create_tables(stop_thread_var):
    global running_thread, connect_method
    return

def submit_insert_gtfs(stop_thread_var):
    global connect_method, gtfs_insert_method
    if validate_entries( 
        [   (db_host, "Host"),
            (db_port, "Port"),
            (db_service_name, "Service-Name"),
            (db_username, "Username"),
            (db_password, "Password"),
            (gtfs_path, "GTFS-Pfad"),
            (batch_size, "Batch-Size")   ]
    ):
        # Leere die Konsolenausgabe
        console_text.configure(state="normal")
        console_text.delete(1.0, tk.END)
        console_text.configure(state="disabled")

        # Starte die Connect-Methode in einem eigenen Thread
        oracle_db_connection = connect_method(
            db_host.get(),
            db_port.get(),
            db_service_name.get(),
            db_username.get(),
            db_password.get(),
        )

        if stop_thread_var.get(): return

        gtfs_insert_method(
            oracle_db_connection, 
            gtfs_path.get(), 
            int(batch_size.get()),
            stop_thread_var
        )

def on_cancel(root, stop_thread_var):
    stop_thread(root, stop_thread_var, running_thread)
    root.quit()

def load_config_to_fields():
    config = get_config()
    if config:
        db_config = config.get('database', {})
        set_entry_value(db_host, db_config.get('host', ''), tk)
        set_entry_value(db_port, db_config.get('port', ''), tk)
        set_entry_value(db_service_name, db_config.get('service-name', ''), tk)
        set_entry_value(db_username, db_config.get('username', ''), tk)
        set_entry_value(db_password, db_config.get('password', ''), tk)
        set_entry_value(gtfs_path, config.get('input_directory', ''), tk)
        set_entry_value(batch_size, config.get('batch_size', ''), tk)

def select_gtfs_path():
    folder_path = filedialog.askdirectory(title="Ordner auswählen")
    if folder_path:
        set_entry_value(gtfs_path, folder_path, tk)

def create_label_entry(parent, text, row, show=None, validate_command=None):
    ttk.Label(parent, text=text).grid(row=row, column=0, padx=10, pady=5, sticky=tk.W)
    entry = ttk.Entry(parent, show=show, validate="key", validatecommand=validate_command)
    entry.grid(row=row, column=1, padx=10, pady=5, sticky=tk.EW)
    return entry

def start_user_interface(get_db_connection, gtfs_to_inserts):
    global db_host, db_port, db_service_name, db_username, db_password, batch_size, gtfs_path, running_thread, console_text
    global mode, connect_method, gtfs_insert_method

    connect_method = get_db_connection
    gtfs_insert_method = gtfs_to_inserts
    
    global db_config_frame, gtfs_path_frame, batch_size_frame
    
    running_thread = None

    mode = ModeEnum.INSERT_GTFS

    root = tk.Tk()

    # Variable, um den Thread zu stoppen
    stop_thread_var = tk.BooleanVar(value=False)

    # Hauptfenster erstellen
    root.title("gtfsToOracleDB")
    root.geometry("1587x764")

    # Validierungsfunktion für Ganzzahlen
    vcmd = (root.register(validate_int_input), '%P')

    # Gesamtframe für Eingaben
    input_frame = LabelFrame(root, "Eingabe", tk, ttk).set_padding((4, 1)).set_columnspan(1).set_sticky(tk.NSEW).build()

    # Konfiguriere die Zeilen im input_frame
    input_frame.rowconfigure(0, weight=0)  # actions_frame (fixe Höhe)
    input_frame.rowconfigure(1, weight=1)  # Restliche Elemente (flexibel)

    # Frame für die Auswahl der Aktionen
    actions_frame = LabelFrame(input_frame, "Aktionen", tk, ttk).set_row(0).set_padding((5, 5)).build()

    # Buttons für die Aktionen
    button_width = 24  # Feste Breite für alle Buttons
    ttk.Button(actions_frame, text="CREATE TABLES", style="Orange.TButton", width=button_width, command=lambda: set_create_tables_mode()).grid(row=0, column=0, padx=5, pady=2)
    ttk.Button(actions_frame, text="INSERT GTFS", style="Orange.TButton", width=button_width, command=lambda: set_insert_gtfs_mode()).grid(row=0, column=1, padx=5, pady=2)
    ttk.Button(actions_frame, text="RELATIONAL TO GRAPH", style="Orange.TButton", width=button_width, command=lambda: set_relational_to_graph_mode()).grid(row=1, column=0, padx=5, pady=4)
    ttk.Button(actions_frame, text="PERFORMANCE ANALYSIS", style="Orange.TButton", width=button_width, command=lambda: set_performance_analysis_mode()).grid(row=1, column=1, padx=5, pady=4)

    action_data_frame = LabelFrame(input_frame, "Aktions-Daten", tk, ttk).set_row(1).build()

    # Labels und Eingabefelder für Datenbankkonfiguration
    db_config_frame = LabelFrame(action_data_frame, "OracleDB Konfiguration", tk, ttk).set_row(0).build()
    db_host = create_label_entry(db_config_frame, "Host:", 0)
    db_port = create_label_entry(db_config_frame, "Port:", 1, validate_command=vcmd)
    db_service_name = create_label_entry(db_config_frame, "Service-name:", 2)
    db_username = create_label_entry(db_config_frame, "Username:", 3)
    db_password = create_label_entry(db_config_frame, "Password:", 4, show="*")

    # Elemente für Auswahl des GTFS-Ordners
    gtfs_path_frame = LabelFrame(action_data_frame, "Dateipfad zu GTFS-Dateien", tk, ttk).set_row(1).build()
    gtfs_path = ttk.Entry(gtfs_path_frame)
    gtfs_path.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)
    ttk.Button(gtfs_path_frame, text="Ordner auswählen", command=select_gtfs_path).grid(row=0, column=1, padx=10, pady=10)

    # Eingabe für Batch-Größe
    batch_size_frame = LabelFrame(action_data_frame, "Batch-Größe", tk, ttk).set_row(2).build()
    batch_size = create_label_entry(batch_size_frame, "Batch-Size:", 2, validate_command=vcmd)

    # Buttons für Verwaltung der Konfigurationsdatei
    ttk.Button(input_frame, text="Konfigurationsdatei erstellen", command=create_config).grid(row=2, column=0, padx=10, pady=10)
    ttk.Button(input_frame, text="Konfigurationsdatei laden", command=load_config_to_fields).grid(row=2, column=1, padx=10, pady=10)

    # Buttons für Starten des Imports und Abbrechen des Imports
    ttk.Button(input_frame, text="Abbrechen", command=lambda: stop_thread(root, stop_thread_var, running_thread), style="Red.TButton").grid(row=3, column=0, padx=10, pady=10)
    ttk.Button(input_frame, text="Bestätigen", command=lambda: on_submit(stop_thread_var), style="Green.TButton").grid(row=3, column=1, padx=10, pady=10)

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


def set_create_tables_mode():
    global mode
    mode = ModeEnum.CREATE_TABLES
    update_visibility()

def set_insert_gtfs_mode():
    global mode
    mode = ModeEnum.INSERT_GTFS
    update_visibility()

def set_relational_to_graph_mode():
    global mode
    mode = ModeEnum.RELATIONAL_TO_GRAPH
    update_visibility()

def set_performance_analysis_mode():
    global mode
    mode = ModeEnum.PERFORMANCE_ANALYSIS
    update_visibility()


def toggle_visibility(widget, visible_when: list[ModeEnum]):
    global mode
    if mode in visible_when:
        widget.grid()  # Zeigt das Element wieder an
    else:
        widget.grid_forget()  # Blendet das Element aus

def update_visibility():
    toggle_visibility(db_config_frame, [ModeEnum.CREATE_TABLES, ModeEnum.INSERT_GTFS])
    toggle_visibility(gtfs_path_frame, [ModeEnum.INSERT_GTFS])
    toggle_visibility(batch_size_frame, [ModeEnum.INSERT_GTFS])

