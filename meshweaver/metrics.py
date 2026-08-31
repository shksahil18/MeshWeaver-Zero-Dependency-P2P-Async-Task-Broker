import os
import platform
import time


class SystemMetrics:
    """
    Cross-platform CPU and RAM usage collector.

    No external dependency is required.
    """

    def __init__(self):
        self._previous_cpu = None

    def cpu_percent(self) -> float:
        """
        Return approximate system CPU utilization.

        Linux:
            Reads /proc/stat.

        Windows:
            Uses GetSystemTimes through ctypes.

        Other platforms:
            Uses load average when available as a fallback.
        """

        system = platform.system()

        if system == "Linux":
            return self._linux_cpu_percent()

        if system == "Windows":
            return self._windows_cpu_percent()

        return self._fallback_cpu_percent()

    def memory_percent(self) -> float:
        """
        Return system RAM utilization percentage.
        """

        system = platform.system()

        if system == "Linux":
            return self._linux_memory_percent()

        if system == "Windows":
            return self._windows_memory_percent()

        return self._fallback_memory_percent()

    def snapshot(self) -> dict:
        """
        Return a CPU/RAM metrics snapshot.
        """

        return {
            "cpu_percent": round(self.cpu_percent(), 2),
            "memory_percent": round(
                self.memory_percent(),
                2,
            ),
        }

    def _linux_cpu_times(self):
        with open("/proc/stat", "r", encoding="utf-8") as file:
            line = file.readline()

        values = line.split()[1:]

        return [int(value) for value in values]

    def _linux_cpu_percent(self) -> float:
        current = self._linux_cpu_times()

        if self._previous_cpu is None:
            self._previous_cpu = current

            time.sleep(0.05)

            current = self._linux_cpu_times()

        previous = self._previous_cpu
        self._previous_cpu = current

        previous_total = sum(previous)
        current_total = sum(current)

        previous_idle = previous[3]
        current_idle = current[3]

        total_delta = current_total - previous_total
        idle_delta = current_idle - previous_idle

        if total_delta <= 0:
            return 0.0

        usage = (
            1 - (idle_delta / total_delta)
        ) * 100

        return max(0.0, min(100.0, usage))

    def _linux_memory_percent(self) -> float:
        memory = {}

        with open(
            "/proc/meminfo",
            "r",
            encoding="utf-8",
        ) as file:

            for line in file:
                key, value = line.split(":", 1)
                memory[key] = int(
                    value.strip().split()[0]
                )

        total = memory.get("MemTotal", 0)
        available = memory.get("MemAvailable", 0)

        if total == 0:
            return 0.0

        used = total - available

        return (used / total) * 100

    def _windows_cpu_percent(self) -> float:
        import ctypes
        from ctypes import wintypes

        class FILETIME(ctypes.Structure):
            _fields_ = [
                ("dwLowDateTime", wintypes.DWORD),
                ("dwHighDateTime", wintypes.DWORD),
            ]

        idle = FILETIME()
        kernel = FILETIME()
        user = FILETIME()

        result = ctypes.windll.kernel32.GetSystemTimes(
            ctypes.byref(idle),
            ctypes.byref(kernel),
            ctypes.byref(user),
        )

        if not result:
            return 0.0

        def filetime_to_int(filetime):
            return (
                filetime.dwHighDateTime << 32
            ) + filetime.dwLowDateTime

        idle_time = filetime_to_int(idle)
        kernel_time = filetime_to_int(kernel)
        user_time = filetime_to_int(user)

        current = (
            idle_time,
            kernel_time,
            user_time,
        )

        if self._previous_cpu is None:
            self._previous_cpu = current

            time.sleep(0.1)

            return self._windows_cpu_percent()

        previous = self._previous_cpu
        self._previous_cpu = current

        idle_delta = current[0] - previous[0]

        total_delta = (
            (current[1] - previous[1])
            + (current[2] - previous[2])
        )

        if total_delta <= 0:
            return 0.0

        usage = (
            1 - idle_delta / total_delta
        ) * 100

        return max(0.0, min(100.0, usage))

    def _windows_memory_percent(self) -> float:
        import ctypes
        from ctypes import wintypes

        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", wintypes.DWORD),
                ("dwMemoryLoad", wintypes.DWORD),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MEMORYSTATUSEX()
        status.dwLength = ctypes.sizeof(
            MEMORYSTATUSEX
        )

        result = (
            ctypes.windll.kernel32
            .GlobalMemoryStatusEx(
                ctypes.byref(status)
            )
        )

        if not result:
            return 0.0

        return float(status.dwMemoryLoad)

    def _fallback_cpu_percent(self) -> float:
        try:
            load = os.getloadavg()[0]
            cpu_count = os.cpu_count() or 1

            usage = (
                load / cpu_count
            ) * 100

            return max(
                0.0,
                min(100.0, usage),
            )

        except (AttributeError, OSError):
            return 0.0

    def _fallback_memory_percent(self) -> float:
        return 0.0