from tkinter import messagebox, Entry

def validate_entries(
        fields: list[(Entry, str)],
        batch_size: Entry
):
    for field, name in fields:
        if not field.get():
            messagebox.showerror("Fehler", f"{name} darf nicht leer sein.")
            return False

    if not batch_size.get().isdigit():
        messagebox.showerror("Fehler", "Batch-Size muss eine Zahl sein.")
        return False

    return True

def validate_int_input(P):
    if P.isdigit() or P == "":
        return True
    else:
        return False