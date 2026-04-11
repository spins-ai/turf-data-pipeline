#!/usr/bin/env python3
"""Quick RAM check - prints usage and warns if over limit."""
import ctypes, sys
class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [('dwLength', ctypes.c_ulong), ('dwMemoryLoad', ctypes.c_ulong),
                ('ullTotalPhys', ctypes.c_ulonglong), ('ullAvailPhys', ctypes.c_ulonglong),
                ('ullTotalPageFile', ctypes.c_ulonglong), ('ullAvailPageFile', ctypes.c_ulonglong),
                ('ullTotalVirtual', ctypes.c_ulonglong), ('ullAvailVirtual', ctypes.c_ulonglong),
                ('ullAvailExtendedVirtual', ctypes.c_ulonglong)]
stat = MEMORYSTATUSEX(); stat.dwLength = ctypes.sizeof(stat)
ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
total = stat.ullTotalPhys / 1024**3; used = total - stat.ullAvailPhys / 1024**3
print(f"RAM: {used:.1f}/{total:.1f} Go ({stat.dwMemoryLoad}%) | Marge: {57-used:.1f} Go")
if used > 50: print("ALERTE: >50 Go!"); sys.exit(1)
