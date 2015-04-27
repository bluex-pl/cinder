import uuid

from oslo_config import cfg
from oslo_log import log
import tooz.coordination
import tooz.locking

from cinder.i18n import _LE, _LI

LOG = log.getLogger(__name__)

OPTS = [
    cfg.StrOpt('backend_url',
               default=None,
               help='The backend URL to use for distributed coordination. If '
                    'left empty, per-deployment central agent and per-host '
                    'compute agent won\'t do workload '
                    'partitioning and will only function correctly if a '
                    'single instance of that service is running.'),
    cfg.FloatOpt('heartbeat',
                 default=1.0,
                 help='Number of seconds between heartbeats for distributed '
                      'coordination.'),

]
cfg.CONF.register_opts(OPTS, group='coordination')


class Lock(tooz.locking.Lock):
    def __init__(self, lock):
        self._lock = lock

    def acquire(self, blocking=True):
        try:
            return self._lock.acquire(blocking)
        except tooz.coordination.ToozError:
            LOG.exception(_LE('Error while acquiring lock.'))

    def release(self):
        try:
            return self._lock.release()
        except tooz.coordination.ToozError:
            LOG.exception(_LE('Error while releasing lock.'))

    def __getattr__(self, name):
        return getattr(self._lock, name)


class Coordinator(object):
    def __init__(self, my_id=None):
        self._coordinator = None
        self._my_id = my_id or str(uuid.uuid4())
        self._started = False

    def start(self):
        backend_url = cfg.CONF.coordination.backend_url
        if backend_url:
            try:
                self._coordinator = tooz.coordination.get_coordinator(
                    backend_url, self._my_id)
                self._coordinator.start()
                self._started = True
                LOG.info(_LI('Coordination backend started successfully.'))
            except tooz.coordination.ToozError:
                self._started = False
                LOG.exception(_LE('Error connecting to coordination backend.'))

    def stop(self):
        if not self._coordinator:
            return

        try:
            self._coordinator.stop()
        except tooz.coordination.ToozError:
            LOG.exception(_LE('Error connecting to coordination backend.'))
        finally:
            self._coordinator = None
            self._started = False

    def is_active(self):
        return self._coordinator is not None

    def heartbeat(self):
        if self._coordinator:
            if not self._started:
                # re-connect
                self.start()
            try:
                #if self._coordinator._acquired_locks:
                    #from pudb import set_trace; set_trace()
                    #import rpdb; rpdb.set_trace()
                self._coordinator.heartbeat()
            except tooz.coordination.ToozError:
                LOG.exception(_LE('Error sending a heartbeat to coordination '
                                  'backend.'))

    def get_lock(self, name):
        return Lock(self._coordinator.get_lock(name))