from collections import OrderedDict
from typing import Dict

from rich.live import Live
from rich.text import Text

from ai.chronon.cli.compile.display.class_tracker import ClassTracker
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj


class CompileStatus:
    """
    Uses rich ui - to consolidate and sink the overview of the compile process to the bottom.
    """

    def __init__(self, use_live: bool = False):
        self.cls_to_tracker: Dict[str, ClassTracker] = OrderedDict()
        self.use_live = use_live
        # we need vertical_overflow to be visible as the output gets cufoff when our output goes past the termianal window
        # but then we start seeing duplicates: https://github.com/Textualize/rich/issues/3263
        if self.use_live:
            self.live = Live(refresh_per_second=50, vertical_overflow="visible")
            self.live.start()

    def print_live_console(self, msg: str):
        if self.use_live:
            self.live.console.print(msg)

    def add_object_update_display(self, compiled: CompiledObj, obj_type: str = None) -> None:
        if compiled.obj_type is not None and obj_type is not None:
            assert compiled.obj_type == obj_type, (
                f"obj_type mismatch: {compiled.obj_type} != {obj_type}"
            )

        if obj_type not in self.cls_to_tracker:
            self.cls_to_tracker[obj_type] = ClassTracker()

        self.cls_to_tracker[obj_type].add(compiled)

        self._update_display()

    def add_existing_object_update_display(self, existing_obj: CompiledObj) -> None:
        obj_type = existing_obj.obj_type

        if obj_type not in self.cls_to_tracker:
            self.cls_to_tracker[obj_type] = ClassTracker()

        self.cls_to_tracker[obj_type].add_existing(existing_obj)

        self._update_display()

    def close_cls(self, obj_type: str) -> None:
        if obj_type in self.cls_to_tracker:
            self.cls_to_tracker[obj_type].close()
            self._update_display()

    def close(self) -> None:
        self._update_display()
        if self.use_live:
            self.live.stop()

    def render(self, ignore_python_errors: bool = False) -> Text:
        text = Text(overflow="fold", no_wrap=False)

        for obj_type, tracker in self.cls_to_tracker.items():
            # Skip MetaData section
            if obj_type == "MetaData":
                continue

            text.append(f"\n{obj_type}-s:\n", style="cyan")

            status = tracker.to_status()
            if status:
                text.append(status)

            errors = tracker.to_errors()
            if errors:
                text.append(errors)

            diff = tracker.diff(ignore_python_errors)
            if diff:
                text.append(diff)

        text.append("\n")
        return text

    def _update_display(self):
        # self.live.clear()

        # TODO: add this after live_crop is implemented
        # text = self.display_text()
        # if self.use_live:
        #     self.live.update(text, refresh=True)
        # return text
        pass
