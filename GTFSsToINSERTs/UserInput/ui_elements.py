class LabelFrame:
    def __init__(self, parent, text, tk, ttk):
        self.tk = tk
        self.ttk = ttk
        self.parent = parent
        self.text = text
        self.padding = (10, 10)
        self.padx = 10
        self.pady = 3
        self.row = 0
        self.column = 0
        self.columnspan = 2
        self.sticky = tk.EW
        self.width = None

    def set_padding(self, padding:(int, int)): # type: ignore
        self.padding = padding
        return self
    
    def set_columnspan(self, columnspan:int):
        self.columnspan = columnspan
        return self
    
    def set_sticky(self, sticky):
        self.sticky = sticky
        return self
    
    def set_row(self, row:int):
        self.row = row
        return self
    
    def set_column(self, column:int):
        self.column = column
        return self
    
    def set_width(self, width:int):
        self.width = width
        return self

    def build(self):
        if self.width:
            label_frame = self.ttk.LabelFrame(self.parent, text=self.text, padding=self.padding, width=self.width)
        else:
            label_frame = self.ttk.LabelFrame(self.parent, text=self.text, padding=self.padding)
        label_frame.grid(row=self.row, column=self.column, columnspan=self.columnspan, padx=self.padx, pady=self.pady, sticky=self.sticky)
        return label_frame

from PIL import Image, ImageTk

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


def Style(ttk):
    style = ttk.Style()

    # Passe den Standard-Stil für alle Buttons an
    style.configure("TButton",
                    background="#478ced",
                    foreground="black",
                    font=("Arial", 8, "bold"))
    style.map("TButton",
            background=[("active", "#3931b5")])
    
    # Erstelle einen orangenen Button-Stil
    style.configure("Orange.TButton",
                    background="#cc7212",
                    foreground="black",
                    font=("Arial", 8, "bold"))
    style.map("Orange.TButton",
            background=[("active", "#ed800c")])

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

def set_entry_value(entry, value, tk):
    entry.delete(0, tk.END)
    entry.insert(0, value)