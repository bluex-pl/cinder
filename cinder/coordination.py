from functools import wraps
import uuid

from oslo_config import cfg
from oslo_log import log
from tooz import coordination
from tooz import locking
import eventlet

from cinder.i18n import _LE, _LI, _LW

LOG = log.getLogger(__name__)

OPTS = [
    cfg.StrOpt('backend_url',
               default=None,
               help='The backend URL to use for distributed coordination. If '
                    'left empty, per-deployment central agent and per-host '
                    'compute agent will only function correctly if a '
                    'single instance of that service is running.'),
    cfg.FloatOpt('heartbeat',
                 default=1.0,
                 help='Number of seconds between heartbeats for distributed '
                      'coordination.'),

]
cfg.CONF.register_opts(OPTS, group='coordination')


class Lock(locking.Lock):
    def __init__(self, lock):
        self._lock = lock

    def acquire(self, blocking=True):
        try:
            return self._lock.acquire(blocking)
        except coordination.ToozError:
            LOG.exception(_LE('Error while acquiring lock.'))

    def release(self):
        try:
            return self._lock.release()
        except coordination.ToozError:
            LOG.exception(_LE('Error while releasing lock.'))

    def __getattr__(self, name):
        return getattr(self._lock, name)


class Coordinator(object):
    def __init__(self, my_id=None, prefix=None):
        self.coordinator = None
        self.my_id = my_id or str(uuid.uuid4())
        self.started = False
        self.prefix = prefix

    def is_active(self):
        return self.coordinator is not None

    def start(self):
        if not self.started:
            self._start()
            if self.started:
                eventlet.spawn_n(self._heartbeat)

    def _start(self):
        backend_url = cfg.CONF.coordination.backend_url
        if not backend_url:
            LOG.warning(_LW('Coordination backend not configured.'))
            return
        try:
            member_id = '-'.join(s for s in (
                self.prefix,
                self.my_id
            ) if s)
            self.coordinator = coordination.get_coordinator(
                backend_url, member_id)
            self.coordinator.start()
        except coordination.ToozError:
            self.started = False
            LOG.exception(_LE('Error connecting to coordination backend.'))
        else:
            self.started = True
            LOG.info(_LI('Coordination backend started successfully.'))

    def stop(self):
        if not self.coordinator:
            return

        coordinator, self.coordinator = self.coordinator, None
        try:
            coordinator.stop()
        except coordination.ToozError:
            LOG.exception(_LE('Error connecting to coordination backend.'))
        finally:
            self.started = False

    def _heartbeat(self):
        while self.coordinator:
            if not self.started:
                # re-connect
                self._start()
            try:
                self.coordinator.heartbeat()
            except coordination.ToozError:
                LOG.exception(_LE('Error sending a heartbeat to coordination '
                                  'backend.'))
            else:
                eventlet.sleep(cfg.CONF.coordination.heartbeat)

    def get_lock(self, name, external=False):
        if self.coordinator is not None:
            name = '-'.join(s for s in (
                self.prefix,
                not external and self.my_id,
                name
            ) if s)
            return Lock(self.coordinator.get_lock(name))
        else:
            LOG.warning(_LW('Unable to create lock.'))


COORDINATOR = Coordinator(prefix='cinder')


def lock(lock_name, external=False, coordinator=COORDINATOR):
    def wrap(f):
        @wraps(f)
        def wrapped(*a, **k):
            with coordinator.get_lock(lock_name, external):
                return f(*a, **k)
        return wrapped
    return wrap
