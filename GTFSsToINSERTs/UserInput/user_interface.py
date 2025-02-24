import tkinter as tk
from tkinter import messagebox, filedialog
from tkinter import ttk
from UserInput.import_config import get_config
from UserInput.create_config import create_config

def on_submit():
    if validate_entries():
        root.quit()

def on_cancel():
    root.quit()
    global cancel
    cancel = True

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

def start_user_interface():
    global root, db_host, db_port, db_service_name, db_username, db_password, batch_size, gtfs_path, cancel
    cancel = False

    # Hauptfenster erstellen
    root = tk.Tk()
    root.title("gtfs2OracleDB")
    root.geometry("362x558")

    # Validierungsfunktion für Ganzzahlen
    vcmd = (root.register(validate_int_input), '%P')

    # Frame für Datenbankkonfiguration
    db_config_frame = ttk.LabelFrame(root, text="OracleDB Konfiguration", padding=(10, 10))
    db_config_frame.grid(row=0, column=0, columnspan=2, padx=10, pady=10, sticky=tk.EW)

    # Labels und Eingabefelder für Datenbankkonfiguration
    db_host = create_label_entry(db_config_frame, "Host:", 0)
    db_port = create_label_entry(db_config_frame, "Port:", 1, validate_command=vcmd)
    db_service_name = create_label_entry(db_config_frame, "Service-name:", 2)
    db_username = create_label_entry(db_config_frame, "Username:", 3)
    db_password = create_label_entry(db_config_frame, "Password:", 4, show="*")

    # Frame für GTFS-Pfad
    gtfs_path_frame = ttk.LabelFrame(root, text="Dateipfad zu GTFS-Dateien", padding=(10, 10))
    gtfs_path_frame.grid(row=1, column=0, columnspan=2, padx=10, pady=10, sticky=tk.EW)

    gtfs_path = ttk.Entry(gtfs_path_frame)
    gtfs_path.grid(row=0, column=0, padx=10, pady=5, sticky=tk.EW)

    select_gtfs_path_button = ttk.Button(gtfs_path_frame, text="Ordner auswählen", command=select_gtfs_path)
    select_gtfs_path_button.grid(row=0, column=1, padx=10, pady=10)

    # Frame für Batch-Size
    batch_size_frame = ttk.LabelFrame(root, text="Batch-Größe", padding=(10, 10))
    batch_size_frame.grid(row=2, column=0, columnspan=2, padx=10, pady=10, sticky=tk.EW)

    ttk.Label(batch_size_frame, text="Batch-Size:").grid(row=2, column=0, padx=10, pady=5, sticky=tk.W)
    batch_size = ttk.Entry(batch_size_frame, validate="key", validatecommand=vcmd)
    batch_size.grid(row=2, column=1, padx=10, pady=5, sticky=tk.EW)

    # Buttons
    create_config_button = ttk.Button(root, text="Konfigurationsdatei erstellen", command=create_config)
    create_config_button.grid(row=3, column=0, padx=10, pady=10)

    select_config_button = ttk.Button(root, text="Konfigurationsdatei laden", command=load_config_to_fields)
    select_config_button.grid(row=3, column=1, padx=10, pady=10)

    cancel_button = ttk.Button(root, text="Abbrechen", command=on_cancel, style="Red.TButton")
    cancel_button.grid(row=4, column=0, padx=10, pady=10)

    submit_button = ttk.Button(root, text="Bestätigen", command=on_submit,  style="Green.TButton")
    submit_button.grid(row=4, column=1, padx=10, pady=10)

    style = get_style()

    # Verhalten beim Schließen des Fensters definieren
    root.protocol("WM_DELETE_WINDOW", on_cancel)

    # Hauptschleife starten
    root.mainloop()

    if cancel:
        return None

    # Rückgabe der Eingabewerte als Objekt
    return {
        "username": db_username.get(),
        "password": db_password.get(),
        "host": db_host.get(),
        "port": db_port.get(),
        "service_name": db_service_name.get(),
        "gtfs_path": gtfs_path.get(),
        "batch_size": batch_size.get()
    }

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