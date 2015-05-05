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
    def __init__(self, my_id=None):
        self._coordinator = None
        self._my_id = my_id or str(uuid.uuid4())
        self._started = False

    def is_active(self):
        return self._coordinator is not None

    def start(self):
        if not self._started:
            self._start()
            if self._started:
                eventlet.spawn_n(self._heartbeat)

    def _start(self):
        backend_url = cfg.CONF.coordination.backend_url
        if not backend_url:
            LOG.warning(_LW('Coordination backend not configured.'))
            return
        try:
            self._coordinator = coordination.get_coordinator(
                backend_url, self._my_id)
            self._coordinator.start()
        except coordination.ToozError:
            self._started = False
            LOG.exception(_LE('Error connecting to coordination backend.'))
        else:
            self._started = True
            LOG.info(_LI('Coordination backend started successfully.'))

    def stop(self):
        if not self._coordinator:
            return

        coordinator, self._coordinator = self._coordinator, None
        try:
            coordinator.stop()
        except coordination.ToozError:
            LOG.exception(_LE('Error connecting to coordination backend.'))
        finally:
            self._started = False

    def _heartbeat(self):
        while self._coordinator:
            if not self._started:
                # re-connect
                self._start()
            try:
                self._coordinator.heartbeat()
            except coordination.ToozError:
                LOG.exception(_LE('Error sending a heartbeat to coordination '
                                  'backend.'))
            else:
                eventlet.sleep(cfg.CONF.coordination.heartbeat)

    def get_lock(self, name):
        if self._coordinator is None:
            LOG.warning(_LW('Unable to create lock.'))
        else:
            return Lock(self._coordinator.get_lock(name))

COORDINATOR = Coordinator()
