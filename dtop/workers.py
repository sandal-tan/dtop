"""Get data about Dask Workers and display their info in a scene."""

import time
from datetime import datetime as dt
from collections import defaultdict
import threading
from typing import Dict
from dataclasses import dataclass


from asciimatics.screen import Screen
from asciimatics.widgets import Frame, Layout, Text, VerticalDivider, Label, Divider, MultiColumnListBox, Widget
from asciimatics.parsers import AsciimaticsParser
from dask.distributed import Client

BACKGROUND = Screen.COLOUR_BLACK
"""The default background color."""

PALETTE = defaultdict(
    lambda: (Screen.COLOUR_WHITE, Screen.A_NORMAL, BACKGROUND),
    {
        'title': (Screen.COLOUR_MAGENTA, Screen.A_BOLD, BACKGROUND),
        'borders': (Screen.COLOUR_BLUE, Screen.A_NORMAL, BACKGROUND),
    }
)
"""Our custom color scheme."""


@dataclass
class WorkerInfoModel:
    """Information about a Dask worker.

    Args:
        addr: The address of the dask worker
        max_memory: The total amount of memory available to the worker in bytes
        current_memory: The current amount of memory used by the worker in bytes
        executing: The number of tasks executing on the worker
        in_memory: The number of tasks that are in memory on the worker
        ready: The number of tasks that are ready on the worker
        in_flight: The number of tasks that are inflight on the worker

    """

    addr: str
    max_memory: int
    current_memory: int
    cpu: float
    fds: int
    executing: int
    in_memory: int
    ready: int
    in_flight: int

    @classmethod
    def from_worker(cls: 'WorkerInfoModel', addr: str, info: Dict) -> 'WorkerInfoModel':
        """Create a `WorkerInfo` object from a worker info dictionary.

        Args:
            addr: The address of the Dask worker
            info: The dictionary containing the Dask worker information

        Returns:
            The `WorkerInfo` object from the dictionary
        """
        return cls(
            addr=addr,
            max_memory=info['memory_limit'],
            current_memory=info['metrics']['memory'],
            cpu=info['metrics']['cpu'],
            fds=info['metrics']['num_fds'],
            executing=info['metrics']['executing'],
            in_memory=info['metrics']['in_memory'],
            ready=info['metrics']['ready'],
            in_flight=info['metrics']['in_flight'],
        )

    @property
    def memory_util(self) -> float:
        """The Dask worker's memory utilization as a percent."""
        return self.current_memory / self.max_memory

class ClusterWorkerInfo:

    def __init__(self, dask_client: Client):
        self._client: Client = dask_client
        self.workers = None

    def _refresh_worker_info(self):
        worker_info = self.client.scheduler_info()['workers']
        self.workers = [WorkerInfo.from_worker(addr, info) for addr, info in worker_info.items()]

    @property
    def worker_count(self):
        return len(self.workers)

    @property
    def worker_info(self):
        info = [
            [worker.addr, str(worker.cpu), str(worker.memory_util), str(worker.fds)]
            for worker in self.workers
        ]
        return info


class WorkerInfoView(Frame):

    def __init__(self, screen, client: Client):
        super(WorkerInfoView, self).__init__(screen, screen.height, screen.width, title='Worker Info', reduce_cpu=False)

        self._last_frame = 0
        self._model = client
        self._model._refresh_worker_info()
        self.data =  {'updated_at': str(dt.now())}
        self.palette = PALETTE

        # Number Of Workers | Average CPU | Average Mem | Total FD | Total Executing | Total In Memory | Total Ready | Total In Flight
        rollup_layout = Layout([100])
        self.add_layout(rollup_layout)
        self.rollup_widget = MultiColumnListBox(
            round(screen.height * .05),
            ['12%'] * 8,
            [],
            titles=['No. Workers', 'Avg. CPU', 'Avg. Mem', 'Ttl Fds', 'Ttl Executing', 'Ttl In Mem', 'Ttl Ready', 'Ttl In Flight'],
            parser=AsciimaticsParser()
        )
        rollup_layout.add_widget(self.rollup_widget)

        div_layout = Layout([100])
        self.add_layout(div_layout)
        div_layout.add_widget(Divider())

        column_layout = Layout([100])
        self.add_layout(column_layout)
        self.worker_widget = MultiColumnListBox(
            round(screen.height * .95),
            ['20%', '10%', '20%', '10%', '10%', '10%', '10%', '10%'],
            [],
            titles=['Worker Address', 'CPU', 'Memory', 'File Descriptors', 'Executing', 'In Memory', 'Ready', 'In Flight'],
            parser=AsciimaticsParser(),
        )
        column_layout.add_widget(self.worker_widget)

        self.fix()

    def _update(self, frame_no):
        if frame_no - self._last_frame >= self.frame_update_count or self._last_frame == 0:

            self._last_frame = frame_no
            self._model._refresh_worker_info()
            worker_options = []
            cpu_total = 0.0
            mem_total = 0.0
            fds_total = 0
            exec_total = 0
            in_mem_total = 0
            ready_total = 0
            in_flight_total = 0
            for idx, worker in enumerate(self._model.workers):
                worker_options.append(
                    (
                        [
                            worker.addr,
                            self._get_cpu(worker),
                            self._get_mem(worker),
                            str(worker.fds),
                            str(worker.executing),
                            str(worker.in_memory),
                            str(worker.ready),
                            str(worker.in_flight)
                        ],
                        idx
                    )
                )

                cpu_total += worker.cpu
                mem_total += worker.memory_util
                fds_total += worker.fds
                exec_total += worker.executing
                in_mem_total += worker.in_memory
                ready_total += worker.ready
                in_flight_total += worker.in_flight


            self.worker_widget.options = worker_options

            num_workers = len(self._model.workers)
            self.rollup_widget.options = [([
                str(num_workers),
                f'{cpu_total / num_workers:.2f}',
                f'{mem_total / num_workers * 100:.2f}%',
                str(fds_total),
                str(exec_total),
                str(in_mem_total),
                str(ready_total),
                str(in_flight_total)], 0)

            ]
        super(WorkerInfoView, self)._update(frame_no)

    @property
    def frame_update_count(self):
        return 20 # 20 == 1 second

    @staticmethod
    def _get_human_readable_mem(byte_count: int) -> str:
        gigabytes = byte_count / 1e9
        if gigabytes > 1:
            return f'{gigabytes:.2f} GB'

        megabytes = byte_count / 1e6
        if megabytes > 1:
            return f'{megabytes:.2f} MB'

    def _get_cpu(self, worker: WorkerInfo) -> str:
        cpu_perc = worker.cpu
        color = ""
        if 40 <= cpu_perc < 75:
            color = "${3}"
        elif 75 <= cpu_perc <= 100:
            color = "${1}"
        elif 100 < cpu_perc:
            color = "${2}"
        return f'{color}{cpu_perc}'

    def _get_mem(self, worker: WorkerInfo) -> str:
        memory_perc = worker.memory_util
        mem_used = self._get_human_readable_mem(worker.current_memory)
        mem_total = self._get_human_readable_mem(worker.max_memory)
        label = f'{memory_perc * 100:.2f}% ({mem_used}/{mem_total})'
        if 0 < memory_perc < 0.5:
            color = ""
        elif 0.5 <= memory_perc < 0.75:
            color = "${3}"
        elif 0.75 <= memory_perc <= 1.0:
            color = "${1}"
        return color + label
