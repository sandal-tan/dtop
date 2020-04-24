
import sys

from dask.distributed import Client
from asciimatics.scene import Scene
from asciimatics.screen import Screen
from asciimatics.widgets import VerticalDivider
from asciimatics.exceptions import ResizeScreenError

from dask_top.workers import ClusterWorkerInfo, WorkerInfoView


def demo(screen: Screen, scene):
    scenes = [
        Scene([WorkerInfoView(screen, workers)], -1, name="Worker Info"),
    ]
    screen.force_update()
    screen.play(scenes, stop_on_resize=True, start_scene=scene, allow_int=True)


c = Client('tcp://127.0.0.1:8786')

workers = ClusterWorkerInfo(c)
last_scene = None
while True:
    try:
        Screen.wrapper(demo, arguments=[last_scene])
        sys.exit(0)
    except ResizeScreenError as e:
        last_scene = e.scenelayout.add_widget(VerticalDivider())
