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


class Lock(tooz.locking.lock):
    pass  # TODO


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
                self._coordinator.heartbeat()
            except tooz.coordination.ToozError:
                LOG.exception(_LE('Error sending a heartbeat to coordination '
                                  'backend.'))

    def get_lock(self, name):
        return self._coordinator.get_lock(name)  # TODO: wrap in custom Lock