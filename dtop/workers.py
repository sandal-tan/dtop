"""Get data about Dask Workers and display their info in a scene."""

import time
from datetime import datetime as dt
from collections import defaultdict
import threading
from typing import Dict, List
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
    },
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

class WorkerInfoManager:
    """Manage refreshes of worker information as well as provide rollups
    and formatting of worker information.

    Args:
        dask_client: The Dask client

    """

    def __init__(self, dask_client: Client):
        self._client: Client = dask_client
        self.workers: List[WorkerInfoModel] = []
        self._refresh()

    def _refresh(self) -> None:
        """Update worker information."""
        worker_info = self._client.scheduler_info()['workers']
        self.workers = [WorkerInfoModel.from_worker(addr, info) for addr, info in worker_info.items()]

    @property
    def worker_count(self):
        """The number of workers in the cluster."""
        return len(self.workers)

class WorkerInfoScene(Frame):
    """The Worker Info Overview Scene.

    Args:
        screen: The screen on which this scene will be displayed
        worker_info_manager: The `WorkerInfoManager` used to render the scene

    """

    def __init__(self, screen, worker_info_manager: WorkerInfoManager):
        super(WorkerInfoScene, self).__init__(screen, screen.height, screen.width, title='Worker Info', reduce_cpu=False)

        self._last_frame = 0
        self._model = worker_info_manager
        self._model._refresh()
        self.data =  {'updated_at': str(dt.now())}
        self.palette = PALETTE

        rollup_layout = Layout([100])
        self.add_layout(rollup_layout)
        self.rollup_widget = MultiColumnListBox(
            round(screen.height * .05),
            ['12%'] * 8,
            [],
            titles=[
                'Worker Count',
                'Average CPU',
                'Average Memory',
                'Total Fds',
                'Total Executing',
                'Total In Mem',
                'Total Ready',
                'Total In Flight',
            ],
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
            [
                '20%',
                '10%',
                '20%',
                '10%',
                '10%',
                '10%',
                '10%',
                '10%',
            ],

            [],
            titles=[
                'Worker Address',
                'CPU',
                'Memory',
                'File Descriptors',
                'Executing',
                'In Memory',
                'Ready',
                'In Flight',
            ],
            parser=AsciimaticsParser(),
        )
        column_layout.add_widget(self.worker_widget)

        self.fix()

    def _update(self, frame_no: int) -> None:
        """Override the method to force a poll of the Dask cluster on a regular interval.

        Args:
            frame_no: The number of the frame that should be rendered

        """
        if frame_no - self._last_frame >= self.frame_update_count or self._last_frame == 0:

            self._last_frame = frame_no
            self._model._refresh()
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
                            self._format_cpu(worker),
                            self._format_mem(worker),
                            str(worker.fds),
                            str(worker.executing),
                            str(worker.in_memory),
                            str(worker.ready),
                            str(worker.in_flight)
                        ],
                        idx,
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
            self.rollup_widget.options = [
                (
                    [
                        str(num_workers),
                        f'{cpu_total / num_workers:.2f}',
                        f'{mem_total / num_workers * 100:.2f}%',
                        str(fds_total),
                        str(exec_total),
                        str(in_mem_total),
                        str(ready_total),
                        str(in_flight_total)
                    ],
                    0,
                 )

            ]
        super(WorkerInfoScene, self)._update(frame_no)

    @property
    def frame_update_count(self) -> int:
        """How often this scene should be updated.

        Returns:
            How many redraws should occur before this scene is updated

        """
        return 20 # 20 == 1 second

    @staticmethod
    def _get_human_readable_byte_count(byte_count: int) -> str:
        """Get a human readable byte count. Currently up to Gigabytes.

        Args:
            byte_count: The non-adjusted byte count to convert

        Returns:
            The adjusted byte count, to make it more human readable

        """
        gigabytes = byte_count / 1e9
        if gigabytes > 1:
            return f'{gigabytes:.2f} GB'

        megabytes = byte_count / 1e6
        if megabytes > 1:
            return f'{megabytes:.2f} MB'


    def _format_cpu(self, worker: WorkerInfoModel) -> str:
        """Format the CPU utilization for a worker.

        Args:
            worker: The `WorkerInfoModel` for which the CPU utilization is formatted

        Returns:
            The formatted CPU utilization

        """
        cpu_perc = worker.cpu
        color = ""
        if 40 <= cpu_perc < 75:
            color = "${3}"
        elif 75 <= cpu_perc <= 100:
            color = "${1}"
        elif 100 < cpu_perc:
            color = "${2}"
        return f'{color}{cpu_perc}'

    def _format_mem(self, worker: WorkerInfoModel) -> str:
        """Format the memory utilization for a worker.

        Args:
            worker: The `WorkerInfoModel` for which the memory utilization is formatted

        Returns:
            The formatted memory utilization

        """
        memory_perc = worker.memory_util
        mem_used = self._get_human_readable_byte_count(worker.current_memory)
        mem_total = self._get_human_readable_byte_count(worker.max_memory)
        mem = f'{memory_perc * 100:.2f}% ({mem_used}/{mem_total})'
        color = ""
        if 0.5 <= memory_perc < 0.75:
            color = "${3}"
        elif 0.75 <= memory_perc <= 1.0:
            color = "${1}"
        return f'{color}{mem}'
