import time
from datetime import datetime as dt
from collections import defaultdict
import threading
from typing import Dict


from asciimatics.screen import Screen
from asciimatics.widgets import Frame, Layout, Text, VerticalDivider, Label, Divider, MultiColumnListBox, Widget
from dask.distributed import Client

BACKGROUND = Screen.COLOUR_BLACK
PALETTE = defaultdict(
    lambda: (Screen.COLOUR_WHITE, Screen.A_NORMAL, BACKGROUND),
    {
        'title': (Screen.COLOUR_CYAN, Screen.A_BOLD, BACKGROUND),
        'borders': (Screen.COLOUR_BLUE, Screen.A_NORMAL, BACKGROUND),
        'low_util': (Screen.COLOUR_GREEN, Screen.A_NORMAL, BACKGROUND),
        'medium_util': (Screen.COLOUR_YELLOW, Screen.A_NORMAL, BACKGROUND),
        'high_util': (Screen.COLOUR_RED, Screen.A_NORMAL, BACKGROUND),
    }
)

class WorkerInfo:

    def __init__(self, addr, info: Dict):
        self.addr = addr
        self.max_memory = info['memory_limit']
        self.current_memory = info['metrics']['memory']
        self.cpu = info['metrics']['cpu']
        self.fds = info['metrics']['num_fds']

    @property
    def memory_util(self):
        return self.current_memory / self.max_memory

class ClusterWorkerInfo:

    def __init__(self, client: Client):
        self.client: Client = client
        self.workers = None

    def _refresh_worker_info(self):
        worker_info = self.client.scheduler_info()['workers']
        self.workers = [WorkerInfo(addr, info) for addr, info in worker_info.items()]

    @property
    def worker_count(self):
        self._refresh_worker_info()
        return len(self.workers)

def poll_for_workers(client, data):
    info_getter = ClusterWorkerInfo(client)
    while True:
        info_getter._refresh_worker_info
        data['worker_info'] = info_getter
        data['updated_at'] = str(dt.now())
        time.sleep(3)


class WorkerInfoView(Frame):

    def __init__(self, screen, client: Client):
        super(WorkerInfoView, self).__init__(screen, screen.height, screen.width, title='Worker Info', reduce_cpu=False)

        self._model = client
        self._model._refresh_worker_info()
        self.data =  {'updated_at': str(dt.now())}
        self.palette = PALETTE
        # | Addr | CPU | Memory | File Descriptors | 
        rollup_layout = Layout([100])
        self.add_layout(rollup_layout)
        rollup_layout.add_widget(Text('Updated At:', 'updated_at'))
        layout = Layout([24, 2, 23, 2, 23, 2, 24])
        self.add_layout(layout)
        layout.add_widget(Label('Address'), 0)
        layout.add_widget(VerticalDivider(), 1)
        layout.add_widget(Label('CPU'), 2)
        layout.add_widget(VerticalDivider(), 3)
        layout.add_widget(Label('Memory'), 4)
        layout.add_widget(VerticalDivider(), 5)
        layout.add_widget(Label('File Descriptors'), 6)

        # TODO look into multi-column layout

        div_layout = Layout([100])
        self.add_layout(div_layout)
        div_layout.add_widget(Divider())
        for worker in self._model.workers:
            row_layout = Layout([24, 2, 23, 2, 23, 2, 24])
            self.add_layout(row_layout)
            row_layout.add_widget(Label(worker.addr), 0)
            row_layout.add_widget(VerticalDivider(), 1)
            row_layout.add_widget(Label(worker.cpu), 2)
            row_layout.add_widget(VerticalDivider(), 3)
            row_layout.add_widget(self._get_memory_label(worker), 4)
            row_layout.add_widget(VerticalDivider(), 5)
            row_layout.add_widget(Label(worker.fds), 6)

        self.fix()

    def _update(self, frame_no):
        self.reset()
        super(WorkerInfoView, self)._update(frame_no)

    @staticmethod
    def _get_human_readable_mem(byte_count: int) -> str:
        gigabytes = byte_count / 1e9
        if gigabytes > 1:
            return f'{gigabytes:.2f} GB'

        megabytes = byte_count / 1e6
        if megabytes > 1:
            return f'{megabytes:.2f} MB'

    def _get_memory_label(self, worker: WorkerInfo) -> str:
        memory_perc = worker.memory_util
        mem_used = self._get_human_readable_mem(worker.current_memory)
        mem_total = self._get_human_readable_mem(worker.max_memory)
        label = Label(f'{memory_perc * 100:.2f}% ({mem_used}/{mem_total})')
        if 0 < memory_perc < 0.5:
            label.custom_colour = 'low_util'
        elif 0.5 <= memory_perc < 0.75:
            label.custom_colour = 'medium_util'
        elif 0.75 <= memory_perc <= 1.0:
            label.custom_colour = 'high_util'
        return label

    def reset(self):
        super(WorkerInfoView, self).reset()
        self._model._refresh_worker_info()
        self.data =  {'updated_at': str(dt.now())}
