# src/iter8/__init__.py

# Make DataSheet available directly from the iter8 package
from .data_sheet import DataSheet

# Define the package version
# This is often read by setup tools or can be used programmatically
__version__ = "0.1.0"

# Optional: Define what '*' imports
__all__ = ['DataSheet'] 