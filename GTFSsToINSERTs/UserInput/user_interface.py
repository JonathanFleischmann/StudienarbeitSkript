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
    GRAPH_ANALYSIS = 3
    RELATIONAL_ANALYSIS = 4


def on_submit(stop_thread_var):
    global running_thread, mode
    if running_thread and running_thread.is_alive():
        print("Es ist bereits eine Operation aktiv...")
        return
    if mode == ModeEnum.CREATE_TABLES:
        # Starte den Thread für das Erstellen der Tabellen
        thread = threading.Thread(target=lambda: submit_create_tables(stop_thread_var), daemon=True)
        thread.start()
        running_thread = thread
    elif mode == ModeEnum.INSERT_GTFS:
        # Starte den Thread für das Einfügen der GTFS-Daten
        thread = threading.Thread(target=lambda: submit_insert_gtfs(stop_thread_var), daemon=True)
        thread.start()
        running_thread = thread

    elif mode == ModeEnum.RELATIONAL_ANALYSIS:
        # Starte den Thread für die Performance-Analyse
        thread = threading.Thread(target=lambda: submit_relational_analysis(stop_thread_var), daemon=True)
        thread.start()
        running_thread = thread

def submit_create_tables(stop_thread_var):
    global running_thread, connect_method, create_tables_method
    if validate_entries(
        [   (db_host, "Host"),
            (db_port, "Port"),
            (db_service_name, "Service-Name"),
            (db_username, "Username"),
            (db_password, "Password")   ]
    ):
        # Leere die Konsolenausgabe
        console_text.configure(state="normal")
        console_text.delete(1.0, tk.END)
        console_text.configure(state="disabled")

        # Starte die Verbindung zur Datenbank
        oracle_db_connection = connect_method(
            db_host.get(),
            db_port.get(),
            db_service_name.get(),
            db_username.get(),
            db_password.get(),
        )

        if stop_thread_var.get(): return

        # Starte das Erstellen der Tabellen
        create_tables_method(
            oracle_db_connection, 
            delete_tables_var.get(),
            stop_thread_var
        )

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

        # Starte die Verbindung zur Datenbank
        oracle_db_connection = connect_method(
            db_host.get(),
            db_port.get(),
            db_service_name.get(),
            db_username.get(),
            db_password.get(),
        )

        if stop_thread_var.get(): return

        # Starte den Import der GTFS-Daten
        gtfs_insert_method(
            oracle_db_connection, 
            gtfs_path.get(), 
            int(batch_size.get()),
            stop_thread_var
        )

def submit_relational_analysis(stop_thread_var):
    global connect_method, oracle_statement_method
    if validate_entries(
        [   (db_host, "Host"),
            (db_port, "Port"),
            (db_service_name, "Service-Name"),
            (db_username, "Username"),
            (db_password, "Password")   ]
    ):
        # Leere die Konsolenausgabe
        console_text.configure(state="normal")
        console_text.delete(1.0, tk.END)
        console_text.configure(state="disabled")

        # Starte die Verbindung zur Datenbank
        oracle_db_connection = connect_method(
            db_host.get(),
            db_port.get(),
            db_service_name.get(),
            db_username.get(),
            db_password.get(),
        )

        if stop_thread_var.get(): return

        # Starte die Performance-Analyse
        statement = performance_statement_text.get("1.0", tk.END).strip()
        result = oracle_statement_method(oracle_db_connection, statement)
        print(result)


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

def start_user_interface(get_db_connection, create_tables_and_triggers, gtfs_to_inserts, execute_statement_on_oracle_db):
    global db_host, db_port, db_service_name, db_username, db_password, delete_tables_var, batch_size, gtfs_path, performance_statement_text, running_thread, console_text
    global mode, connect_method, create_tables_method, gtfs_insert_method, oracle_statement_method

    connect_method = get_db_connection
    create_tables_method = create_tables_and_triggers
    gtfs_insert_method = gtfs_to_inserts
    oracle_statement_method = execute_statement_on_oracle_db
    
    global db_select_frame, db_config_frame, delete_tables_checkbox, gtfs_path_frame, batch_size_frame, relational_analysis_frame
    
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
    input_frame = LabelFrame(root, "Eingabe", tk, ttk).set_padding((0, 1)).set_columnspan(1).set_sticky(tk.NSEW).build()

    # Konfiguriere die Zeilen im input_frame
    input_frame.rowconfigure(0, weight=0)  # fixe Höhe
    input_frame.rowconfigure(1, weight=0)  # fixe Höhe
    input_frame.rowconfigure(2, weight=1)  # flexibel

    # Frame für die Auswahl der Aktionen
    actions_frame = LabelFrame(input_frame, "Aktionen", tk, ttk).set_row(0).set_padding((5, 5)).build()

    # Buttons für die Aktionen
    button_width = 24  # Feste Breite für alle Buttons
    ttk.Button(actions_frame, text="CREATE TABLES", style="Orange.TButton", width=button_width, command=lambda: set_create_tables_mode()).grid(row=0, column=0, padx=5, pady=2)
    ttk.Button(actions_frame, text="INSERT GTFS", style="Orange.TButton", width=button_width, command=lambda: set_insert_gtfs_mode()).grid(row=0, column=1, padx=5, pady=2)
    ttk.Button(actions_frame, text="GRAPH ANALYSIS", style="Orange.TButton", width=button_width, command=lambda: set_graph_analysis_mode()).grid(row=1, column=0, padx=5, pady=4)
    ttk.Button(actions_frame, text="RELATIONAL ANALYSIS", style="Orange.TButton", width=button_width, command=lambda: set_relational_analysis_mode()).grid(row=1, column=1, padx=5, pady=4)

    
    action_data_frame = LabelFrame(input_frame, "Aktions-Daten", tk, ttk).set_padding((0,4)).set_row(2).build()

    # Labels und Eingabefelder für Datenbankkonfiguration
    db_config_frame = LabelFrame(action_data_frame, "OracleDB Konfiguration", tk, ttk).set_width(200).set_row(0).build()
    db_host = create_label_entry(db_config_frame, "Host:", 0)
    db_port = create_label_entry(db_config_frame, "Port:", 1, validate_command=vcmd)
    db_service_name = create_label_entry(db_config_frame, "Service-name:", 2)
    db_username = create_label_entry(db_config_frame, "Username:", 3)
    db_password = create_label_entry(db_config_frame, "Password:", 4, show="*")

    # Checkbox für das Löschen bereits existierender Tabellen bei CreateTables
    delete_tables_var = tk.BooleanVar(value=False)
    delete_tables_checkbox = ttk.Checkbutton(action_data_frame, text="⚠️ Bereits existierende Tabellen mit Inhalt löschen", variable=delete_tables_var)
    delete_tables_checkbox.grid(row=1, column=0, padx=10, pady=0, sticky=tk.W)

    # Elemente für Auswahl des GTFS-Ordners
    gtfs_path_frame = LabelFrame(action_data_frame, "Dateipfad zu GTFS-Dateien", tk, ttk).set_row(1).build()
    gtfs_path = ttk.Entry(gtfs_path_frame)
    gtfs_path.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)
    ttk.Button(gtfs_path_frame, text="Ordner auswählen", command=select_gtfs_path).grid(row=0, column=1, padx=10, pady=10)

    # Eingabe für Batch-Größe
    batch_size_frame = LabelFrame(action_data_frame, "Batch-Größe", tk, ttk).set_row(2).build()
    batch_size = create_label_entry(batch_size_frame, "Batch-Size:", 2, validate_command=vcmd)

    # Eingabe für Statement für Performance-Analyse
    relational_analysis_frame = LabelFrame(action_data_frame, "SQL-Statement für Performance-Analyse auf OracleDB", tk, ttk).set_row(1).set_padding((0,3)).build()
    performance_statement_text = tk.Text(relational_analysis_frame, height=6, width=34, wrap="word")
    performance_statement_text.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)
    performance_statement_scrollbar = ttk.Scrollbar(relational_analysis_frame, command=performance_statement_text.yview)
    performance_statement_scrollbar.grid(row=0, column=1, sticky=tk.NS)
    performance_statement_text.config(yscrollcommand=performance_statement_scrollbar.set)



    # Buttons für Verwaltung der Konfigurationsdatei
    ttk.Button(input_frame, text="Konfigurationsdatei erstellen", command=create_config).grid(row=3, column=0, padx=10, pady=10)
    ttk.Button(input_frame, text="Konfigurationsdatei laden", command=load_config_to_fields).grid(row=3, column=1, padx=10, pady=10)



    # Buttons für Starten der Aktion und Abbrechen der Aktion
    ttk.Button(input_frame, text="Abbrechen", command=lambda: stop_thread(root, stop_thread_var, running_thread), style="Red.TButton").grid(row=4, column=0, padx=10, pady=10)
    ttk.Button(input_frame, text="Bestätigen", command=lambda: on_submit(stop_thread_var), style="Green.TButton").grid(row=4, column=1, padx=10, pady=10)




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

    # Sichtbarkeit der UI-Elemente aktualisieren
    update_visibility()

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

def set_graph_analysis_mode():
    global mode
    mode = ModeEnum.GRAPH_ANALYSIS
    update_visibility()

def set_relational_analysis_mode():
    global mode
    mode = ModeEnum.RELATIONAL_ANALYSIS
    update_visibility()


def toggle_visibility(widget, visible_when: list[ModeEnum]):
    global mode
    is_visible = False
    for entry in visible_when:
        if mode == entry:
            is_visible = True
    if is_visible:
        widget.grid()
    else:
        widget.grid_remove()

def update_visibility():
    toggle_visibility(db_config_frame, [ModeEnum.CREATE_TABLES, ModeEnum.INSERT_GTFS, ModeEnum.RELATIONAL_ANALYSIS])
    toggle_visibility(delete_tables_checkbox, [ModeEnum.CREATE_TABLES])
    toggle_visibility(gtfs_path_frame, [ModeEnum.INSERT_GTFS])
    toggle_visibility(batch_size_frame, [ModeEnum.INSERT_GTFS])
    toggle_visibility(relational_analysis_frame, [ModeEnum.RELATIONAL_ANALYSIS])

