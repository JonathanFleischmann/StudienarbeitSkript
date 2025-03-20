def stop_thread(root, stop_thread_var, running_thread):
    if running_thread and running_thread.is_alive():
        print("Operation abbrechen...")
        stop_thread_var.set(True)
        check_thread(root, stop_thread_var, running_thread)
    else:
        print("Kein laufender Thread")

def check_thread(root, stop_thread_var, running_thread):
    """Überprüft, ob der Thread beendet ist, ohne die GUI zu blockieren"""
    if running_thread.is_alive():
        root.after(200, lambda: check_thread(root, stop_thread_var, running_thread))  # In 100ms erneut prüfen
    else:
        print("Operation abgebrochen")
        stop_thread_var.set(False)