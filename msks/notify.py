
import typing
from typing import Callable

import smtplib

from attributee import Attributee, String, Integer, List

from msks import logger


class Channel(Attributee):
    def __call__(self, task: "msks.task.Task", event):
        pass

class ConsoleChannel(Channel):

    filename = String()


    def __call__(self, task: "Task", event):
        from msks.task import TaskStatus

        print("%s: %s" % (task.identifier, task.state))

class FileChannel(Channel):

    filename = String()

    def __call__(self, task: "Task", event):
        from msks.task import TaskStatus

        pass

class SMTPChannel(Channel):

    server = String()
    port = Integer()
    sender = String()
    recipients = List(String())

    def __call__(self, task: "Task", event):
        from msks.task import TaskStatus

        if event != "change" and task.status not in [TaskStatus.COMPLETE, TaskStatus.FAILED]:
            return

        title = "Task {}: {}".format(task.identifier, task.status)

        log = task.log

        lines = log.split("\n")
        message = "\n".join(lines[-100:] if len(lines) > 99 else lines)

        try:
            with smtplib.SMTP(self.server, self.port) as server:
                message = ("Subject: {0}\r\n"
                    "From: {1}\r\n\r\n{3}").format(title,
                    self.sender, message)

                server.sendmail(self.sender, self.recipients, message.encode('utf-8'))
                logger.info("Email send successfuly to %s", self.recipients)
        except smtplib.SMTPException as e:
            logger.error("Unable to send email: %s", e)


def watch(callbacks: typing.List[Callable]):

    from msks.storage import TaskStorage

    tasks = TaskStorage()

    status = {}

    for task in tasks.query():
        status[task.identifier] = task.status

    try:

        while True:
            if not tasks.wait(timeout=5):
                continue

            tasks.update()

            events = []
            processed = []

            for task in tasks.query():
                processed.append(task.identifier)
                if not task.identifier in status:
                    events.append((task, "new"))
                elif task.identifier == status[task.identifier]:
                    continue
                else:
                    events.append((task, "change"))
                status[task.identifier] = task.status

            for rm in list([x for x in status if x not in processed]):
                del status[rm]
                events.append((task, "removed"))

            for event in events:
                for callback in callbacks:
                    callback(*event)

    except KeyboardInterrupt:
        pass
