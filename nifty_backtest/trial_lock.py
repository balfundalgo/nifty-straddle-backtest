# trial_lock.py — License/trial expiry check
# Called once at GUI startup before the main window loads

import base64
import sys
from datetime import date


def check_trial():
    """
    Check if the trial period is still valid.
    Shows expiry dialog and exits if expired.
    Call this as the FIRST thing in gui.py before any window is shown.
    """
    from trial_config import EXPIRY_ENCODED, COMPANY_NAME, CONTACT, PRODUCT_NAME

    try:
        expiry_str  = base64.b64decode(EXPIRY_ENCODED.encode()).decode()
        expiry_date = date.fromisoformat(expiry_str)
        today       = date.today()

        days_left = (expiry_date - today).days

        if today > expiry_date:
            _show_expired_dialog(COMPANY_NAME, CONTACT, PRODUCT_NAME, expiry_str)
            sys.exit(0)

        if days_left <= 3:
            # Warn user trial is about to expire (non-blocking)
            _show_warning_dialog(days_left, COMPANY_NAME, CONTACT, PRODUCT_NAME)

    except Exception:
        # If anything goes wrong reading the lock, fail safe = allow
        pass


def _show_expired_dialog(company, contact, product, expiry_str):
    """Show trial expired dialog using tkinter (always available)."""
    import tkinter as tk
    from tkinter import messagebox

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    msg = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  {product}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"  Your trial period has expired.\n"
        f"  Trial ended on: {expiry_str}\n\n"
        f"  To purchase a full license,\n"
        f"  please contact:\n\n"
        f"  🏢  {company}\n"
        f"  📞  {contact}\n"
    )

    messagebox.showerror(
        title=f"{product} — Trial Expired",
        message=msg
    )
    root.destroy()


def _show_warning_dialog(days_left, company, contact, product):
    """Show non-blocking warning when trial is about to expire."""
    import tkinter as tk
    from tkinter import messagebox

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    day_word = "day" if days_left == 1 else "days"
    msg = (
        f"Your trial expires in {days_left} {day_word}.\n\n"
        f"To purchase a full license:\n\n"
        f"🏢  {company}\n"
        f"📞  {contact}"
    )

    messagebox.showwarning(
        title=f"{product} — Trial Expiring Soon",
        message=msg
    )
    root.destroy()
