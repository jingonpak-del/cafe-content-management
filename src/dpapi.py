from __future__ import annotations

import ctypes
from ctypes import wintypes


class DPAPIError(RuntimeError):
    pass


class DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_char)),
    ]


crypt32 = ctypes.windll.crypt32
kernel32 = ctypes.windll.kernel32

CRYPTPROTECT_UI_FORBIDDEN = 0x01


def _blob_from_bytes(data: bytes) -> DATA_BLOB:
    buf = ctypes.create_string_buffer(data)
    return DATA_BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))


def _bytes_from_blob(blob: DATA_BLOB) -> bytes:
    if not blob.pbData:
        return b""
    try:
        return ctypes.string_at(blob.pbData, blob.cbData)
    finally:
        kernel32.LocalFree(blob.pbData)


def protect(data: bytes, entropy: bytes = b"cafegamsi-tracker") -> bytes:
    in_blob = _blob_from_bytes(data)
    entropy_blob = _blob_from_bytes(entropy)
    out_blob = DATA_BLOB()
    ok = crypt32.CryptProtectData(
        ctypes.byref(in_blob), None, ctypes.byref(entropy_blob),
        None, None, CRYPTPROTECT_UI_FORBIDDEN, ctypes.byref(out_blob),
    )
    if not ok:
        raise DPAPIError(f"CryptProtectData failed: {ctypes.get_last_error()}")
    return _bytes_from_blob(out_blob)


def unprotect(data: bytes, entropy: bytes = b"cafegamsi-tracker") -> bytes:
    in_blob = _blob_from_bytes(data)
    entropy_blob = _blob_from_bytes(entropy)
    out_blob = DATA_BLOB()
    ok = crypt32.CryptUnprotectData(
        ctypes.byref(in_blob), None, ctypes.byref(entropy_blob),
        None, None, CRYPTPROTECT_UI_FORBIDDEN, ctypes.byref(out_blob),
    )
    if not ok:
        raise DPAPIError(f"CryptUnprotectData failed: {ctypes.get_last_error()}")
    return _bytes_from_blob(out_blob)
