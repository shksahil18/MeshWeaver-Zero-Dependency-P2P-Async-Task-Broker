"""
Week 4 – Live CLI Dashboard.

Renders a real-time mesh topology and task execution view
using the ``rich`` library.

Usage
─────
    from meshweaver.dashboard import MeshDashboard

    dashboard = MeshDashboard(node)
    await dashboard.run()           # blocks until CTRL+C

Or start it as a background coroutine:

    task = asyncio.create_task(dashboard.run())

The dashboard refreshes every ``refresh_interval`` seconds (default 1 s).

┌─────────────────────────────────────────────────────────────────┐
│                   MeshWeaver – Live Dashboard                   │
├──────────────────────────┬──────────────────────────────────────┤
│  🌐  Mesh Topology       │  📋  Task Ledger                     │
│                          │                                      │
│  Node       CPU   RAM    │  ID        Fn     Status  Node       │
│  ─────────  ─────  ───── │  ────────  ─────  ──────  ─────────  │
│  9002  ●    12.3%  44.1% │  abc12345  calc   DONE    9002       │
│  9003  ◌    --    --     │  def67890  slow   RUN     9003       │
│                          │                                      │
├──────────────────────────┴──────────────────────────────────────┤
│  Nodes: 2 online / 2 known   Tasks: 3 total  2 done  1 running  │
└─────────────────────────────────────────────────────────────────┘
"""

import asyncio
import time

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich import box
    from rich.columns import Columns
    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Colour palette
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_COLOUR = {
    "PENDING":    "yellow",
    "DISPATCHED": "cyan",
    "COMPLETED":  "green",
    "FAILED":     "red",
    # router route statuses
    "pending":    "yellow",
    "running":    "cyan",
    "completed":  "green",
    "failed":     "red",
}


def _status_text(status: str) -> "Text":
    colour = _STATUS_COLOUR.get(status, "white")
    return Text(status, style=colour)


def _cpu_colour(cpu: float | None) -> str:
    if cpu is None:
        return "white"
    if cpu >= 80:
        return "red"
    if cpu >= 50:
        return "yellow"
    return "green"


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────────────────────────


class MeshDashboard:
    """
    Live CLI dashboard for a running MeshNode.

    Parameters
    ----------
    node             : MeshNode instance
    refresh_interval : seconds between renders (default 1.0)
    max_tasks        : max rows shown in the task ledger (default 20)
    """

    def __init__(
        self,
        node,
        refresh_interval: float = 1.0,
        max_tasks: int = 20,
    ):
        if not _RICH_AVAILABLE:
            raise ImportError(
                "The 'rich' library is required for the dashboard. "
                "Install it with:  pip install rich"
            )

        self.node = node
        self.refresh_interval = refresh_interval
        self.max_tasks = max_tasks
        self._console = Console()

    # ─────────────────────────────────────────────────────────────
    # Public entry-point
    # ─────────────────────────────────────────────────────────────

    async def run(self):
        """
        Start the live dashboard.  Returns when the node stops or
        the coroutine is cancelled.
        """

        with Live(
            self._render(),
            console=self._console,
            refresh_per_second=1,
            screen=True,
        ) as live:

            while self.node.running:

                live.update(self._render())

                await asyncio.sleep(self.refresh_interval)

    def render_once(self) -> "Layout":
        """
        Render a single snapshot layout.  Useful for one-shot display.
        """

        return self._render()

    def print_snapshot(self):
        """
        Print a static snapshot of the dashboard to the terminal.
        """

        self._console.print(self._render())

    # ─────────────────────────────────────────────────────────────
    # Render helpers
    # ─────────────────────────────────────────────────────────────

    def _render(self) -> "Layout":

        layout = Layout()

        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )

        layout["body"].split_row(
            Layout(name="topology", ratio=1),
            Layout(name="tasks", ratio=2),
        )

        layout["header"].update(
            self._header_panel()
        )

        layout["topology"].update(
            self._topology_panel()
        )

        layout["tasks"].update(
            self._tasks_panel()
        )

        layout["footer"].update(
            self._footer_panel()
        )

        return layout

    # ── Header ────────────────────────────────────────────────────

    def _header_panel(self) -> "Panel":

        node_id_short = self.node.node_id_hex[:16] + "..."

        title = Text.assemble(
            ("⬡ MeshWeaver ", "bold cyan"),
            ("Live Dashboard", "bold white"),
            ("  │  Node: ", "white"),
            (node_id_short, "dim cyan"),
            ("  │  ", "white"),
            (f"{self.node.host}:{self.node.port}", "bold magenta"),
        )

        return Panel(title, box=box.HEAVY_EDGE)

    # ── Topology ──────────────────────────────────────────────────

    def _topology_panel(self) -> "Panel":

        table = Table(
            show_header=True,
            header_style="bold white",
            box=box.SIMPLE,
            expand=True,
            show_lines=False,
        )

        table.add_column("Address", style="bold", width=16)
        table.add_column("Status", width=8)
        table.add_column("CPU", justify="right", width=7)
        table.add_column("RAM", justify="right", width=7)
        table.add_column("Seen", justify="right", width=8)

        peers = self.node.dht.known_peers()
        peer_metrics = self.node.get_peer_metrics()

        # Build a metrics lookup keyed by (host, port).
        metrics_by_addr: dict[tuple, dict] = {}

        for node_id, entry in peer_metrics.items():
            addr = entry.get("address")
            if addr:
                addr = (str(addr[0]), int(addr[1]))
                existing = metrics_by_addr.get(addr)
                recv = entry.get("received_at", 0)
                if not existing or existing.get("received_at", 0) < recv:
                    metrics_by_addr[addr] = entry

        seen_addrs: set = set()

        for peer in peers:

            addr = (str(peer.host), int(peer.port))

            if addr in seen_addrs:
                continue

            seen_addrs.add(addr)

            online = self.node.heartbeat.is_online(addr)
            status_icon = "[green]●[/green]" if online else "[red]◌[/red]"
            status_label = Text("ONLINE", style="green") if online else Text("OFFLINE", style="red")

            metrics = metrics_by_addr.get(addr)

            if metrics:
                cpu = float(metrics.get("cpu_percent", 0))
                ram = float(metrics.get("memory_percent", 0))
                recv_at = metrics.get("received_at")
                age_s = time.monotonic() - recv_at if recv_at else None

                cpu_text = Text(
                    f"{cpu:5.1f}%",
                    style=_cpu_colour(cpu),
                )
                ram_text = Text(f"{ram:5.1f}%")
                age_text = (
                    Text(f"{age_s:.0f}s", style="dim")
                    if age_s is not None
                    else Text("--", style="dim")
                )
            else:
                cpu_text = Text("  --   ", style="dim")
                ram_text = Text("  --   ", style="dim")
                age_text = Text("never", style="dim red")

            table.add_row(
                f"{peer.host}:{peer.port}",
                status_label,
                cpu_text,
                ram_text,
                age_text,
            )

        if not peers:
            table.add_row(
                "[dim]No peers yet[/dim]",
                "",
                "",
                "",
                "",
            )

        return Panel(
            table,
            title="[bold cyan]🌐  Mesh Topology[/bold cyan]",
            border_style="cyan",
            box=box.ROUNDED,
        )

    # ── Task Ledger ───────────────────────────────────────────────

    def _tasks_panel(self) -> "Panel":

        table = Table(
            show_header=True,
            header_style="bold white",
            box=box.SIMPLE,
            expand=True,
            show_lines=False,
        )

        table.add_column("Task ID", width=10)
        table.add_column("Function", width=14)
        table.add_column("Status", width=11)
        table.add_column("Attempts", justify="center", width=9)
        table.add_column("Node", width=16)
        table.add_column("Result / Error", width=24)

        records = self.node.tracker.all()

        # Most-recent first, limited to max_tasks.
        records = sorted(
            records,
            key=lambda r: r.created_at,
            reverse=True,
        )[: self.max_tasks]

        for record in records:

            tid = record.task_id[:8]
            fn_name = record.name[:13]
            status_cell = _status_text(record.status.value)

            attempts_text = (
                f"{record.attempts}/{record.max_attempts}"
            )

            if record.assigned_to:
                node_text = (
                    f"{record.assigned_to[0]}:"
                    f"{record.assigned_to[1]}"
                )
            else:
                node_text = "--"

            if record.status.value == "COMPLETED":
                outcome = Text(
                    str(record.result)[:22],
                    style="green",
                )
            elif record.status.value == "FAILED":
                outcome = Text(
                    (record.error or "")[:22],
                    style="red",
                )
            else:
                outcome = Text("...", style="dim")

            table.add_row(
                tid,
                fn_name,
                status_cell,
                attempts_text,
                node_text,
                outcome,
            )

        if not records:
            table.add_row(
                "[dim]No tasks yet[/dim]",
                "",
                "",
                "",
                "",
                "",
            )

        return Panel(
            table,
            title="[bold yellow]📋  Task Ledger[/bold yellow]",
            border_style="yellow",
            box=box.ROUNDED,
        )

    # ── Footer / Summary ──────────────────────────────────────────

    def _footer_panel(self) -> "Panel":

        peers = self.node.dht.known_peers()
        n_total = len(peers)
        n_online = sum(
            1
            for p in peers
            if self.node.heartbeat.is_online(p.address)
        )

        summary = self.node.tracker.summary()

        total_tasks = sum(summary.values())
        completed = summary.get("COMPLETED", 0)
        failed = summary.get("FAILED", 0)
        running = summary.get("DISPATCHED", 0)
        pending = summary.get("PENDING", 0)

        sign_status = (
            "[green]HMAC-SHA256 ON[/green]"
            if self.node._sign_key
            else "[dim]Signing OFF[/dim]"
        )

        text = Text.assemble(
            ("Nodes: ", "white"),
            (f"{n_online} online", "green"),
            (f" / {n_total} known", "dim"),
            ("   │   ", "dim"),
            ("Tasks: ", "white"),
            (f"{total_tasks} total  ", "white"),
            (f"{completed} done  ", "green"),
            (f"{running} running  ", "cyan"),
            (f"{failed} failed  ", "red"),
            (f"{pending} pending", "yellow"),
            ("   │   ", "dim"),
            ("Security: ", "white"),
            sign_status,
        )

        return Panel(text, box=box.HEAVY_EDGE)
