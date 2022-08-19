
from typing import List, Callable

import smtplib

from msks import logger
from msks.task import Task, TaskStatus
from msks.storage import TaskStorage

class EMailNotifications:

    def __init__(self, email, server, port, sender):
        self._email = email
        self._server = server
        self._port= port
        self._from = sender

   # server = String(default="212.235.188.18")
   # port = Integer(default=25)
   # sender = String(default="hostmaster@vicos.si")
   # recipient = String()

    def __call__(self, task: Task, event):

        if event != "change" and task.status not in [TaskStatus.COMPLETE, TaskStatus.FAILED]:
            return

        title = "Task {}: {}".format(task.identifier, task.status)

        log = task.log

        lines = log.split("\n")
        message = "\n".join(lines[-100:] if len(lines) > 99 else lines)

        try:
            with smtplib.SMTP(self._server, self._port) as server:
                message = ("Subject: {0}\r\n"
                    "From: {1} \r\nTo: {2}\r\n\r\n{3}").format(title,
                    self._from, self._email, message)

                server.sendmail(self._from, self._email, message.encode('utf-8'))
                logger.info("Email send successfuly to %s", self._email)
        except smtplib.SMTPException as e:
            logger.error("Unable to send email: %s", e)


def watch(self, callbacks: List[Callable]):

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
