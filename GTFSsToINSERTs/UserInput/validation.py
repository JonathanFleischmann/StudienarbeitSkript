from tkinter import messagebox, Entry

def validate_entries(
        fields: list[(Entry, str)]
):
    for field, name in fields:
        if name == "Batch-Size":
            if not field.get().isdigit():
                messagebox.showerror("Fehler", f"{name} muss eine Zahl sein.")
                return False
        if not field.get():
            messagebox.showerror("Fehler", f"{name} darf nicht leer sein.")
            return False

    return True

def validate_int_input(P):
    if P.isdigit() or P == "":
        return True
    else:
        return False