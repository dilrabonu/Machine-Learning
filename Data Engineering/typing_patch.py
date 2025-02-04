# typing_patch.py
import sys
import typing

# Patch typing imports
class MockTypingIO:
    BinaryIO = typing.IO[bytes]

sys.modules['typing.io'] = MockTypingIO

def patch_typing_imports():
    """
    Global function to patch typing imports
    Call this at the start of your scripts
    """
    pass