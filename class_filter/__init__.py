import os
import streamlit.components.v1 as components

_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
_func = components.declare_component("class_filter", path=_dir)

def class_filter_widget(classes, selected, key=None):
    """
    Render a toggleable icon grid for WoW class selection.

    classes  – list of {"name": str, "icon": data-URI str, "color": hex str}
    selected – list of currently selected class name strings
    key      – Streamlit widget key (use a mode-specific string so each
               bracket type gets its own independent selection state)

    Returns the updated list of selected class names.
    """
    result = _func(classes=classes, selected=selected, key=key, default=selected)
    return result if result is not None else selected
