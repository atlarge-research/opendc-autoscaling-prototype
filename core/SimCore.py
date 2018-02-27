#!/usr/bin/python

"""
SimCore -- a package for generic discrete event-based simulation (DES)

Design notes:
    - we have borrowed as much as possible from the design of
      SimPy, GridSim, and MONARC.
    - we have tried to eliminate the mechanisms from SimPy
      that we felt were not needed for our simulations.
    - we have tried to extend and to generalize the mechanisms from SimPy
      that we felt were critical for our simulations.

Usage:
  SimCore.py [<N_TICKS>]
  SimCore.py -h | --help

Options:
  -h --help         Show this screen.
"""

import logging
import os
import sys

from sortedcontainers import SortedList

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from docopt import docopt
from utils import SimUtils

config_schema = '''
    [experiment]
    ID              = string(default='simcore')
    [simulation]
    N_TICKS         = integer(min=1)
    OUTPUT_DIR.     = string(default='output')
'''.strip().splitlines()

logger = logging.getLogger(__name__)


class Event(object):
    """
    Data-holder for generic events
    Each generic event has:
    ts_arrival -- the moment when it should arrive in the system
    src        -- the event generator
    dest       -- the event receiver
    params     -- application-dependent (in particular, event-dependent) parameters
    """

    def __init__(self, ts_arrival, source, destination, params):
        self.ts_arrival = ts_arrival
        self.src = source
        self.dest = destination
        self.params = params

    def __str__(self):
        return '{0}: {1}'.format(self.__class__, self.__dict__)

    def __eq__(self, other):
        """Checks if other's attributes have the same value."""

        if self.__class__ != other.__class__:
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __cmp__(self, other):
        return 1 if self.params['type'] > other.params['type'] else -1


class EventQueue(object):
    """
    EventQueue -- an event priority queue
    Notes:
    1. Events are ordered by their arrival time
    2. Events with the same arrival time are sorted by the time they where
       inserted in the queue; another ordering could be to shuffle the set
       of events with the same arrival time, but this can be implemented
       externally and independently from this class.
    """

    def __init__(self):
        self.timestamps = SortedList()  # sorted list of timestamps for which events have been registered
        self.events = {}

        self.count_events_in= 0
        self.count_events_out = 0
        self.count_events_peek = 0

    def __len__(self):
        return self.count_events_in - self.count_events_out

    def enqueue(self, event):
        """Adds one event to the queue."""

        timestamp_arrival = event.ts_arrival

        # Searching a list for an item is linear-time, while searching a dict for an item is constant time.
        # Src: http://jaynes.colorado.edu/PythonIdioms.html
        if timestamp_arrival not in self.timestamps:  # O(log(n)), n number of timestamps
            self.timestamps.add(timestamp_arrival)  # insert and sort O(log n), n number of timestamps
            self.events[timestamp_arrival] = SortedList()

        # avoid appending identical events one after another
        if not self.events[timestamp_arrival] or self.events[timestamp_arrival][-1] != event:
            self.events[timestamp_arrival].add(event)
            self.count_events_in += 1

    def dequeue(self):
        """Returns (and removes from the queue) the next event."""

        if not self.__len__():
            raise IndexError('dequeue from empty EventQueue')

        self.count_events_out += 1

        first_timestamp = self.timestamps[0]
        event_queue = self.events.get(first_timestamp)
        next_event = event_queue.pop(0)

        if not event_queue:  # no more events for this time stamp
            del self.events[first_timestamp]
            del self.timestamps[0]

        return next_event

    def peek(self):
        """Returns (get but does not remove from the queue) the next event."""

        self.count_events_peek += 1

        if not self.__len__():
            raise IndexError('peek in empty EventQueue')

        first_timestamp = self.timestamps[0]
        event_queue = self.events.get(first_timestamp)
        return event_queue[0]


class SimEntity(object):
    """
    SimEntity -- a generic simulated entity

    Notes:
    1. Each simulated entity knows about a global Simulation object, which
       holds, amongst others, the global clock, and references to all the
       objects involved in the simulation (events, all simulated entities).
       This sort of tight-coupling is not really good, but will do for now.
    2. Each simulated entity must have a unique identifier; it is assumed that
       the user of this package will setup correctly her own simulations, by
       implementing a correct identification mechanism.
    3. Each simulated entity responds to the events which it defines (in the
       self.events_map class field). See below for information on how to define
       a new event (and the response to it -- from hereon, handler).
       [Note: this generalizes the yield mechanism from SimPy]
    4. Each simulated entity has an activate member function, which can be used
       to generate the first event for the entity. [Note:idea borrowed from SimPy]

    To add an event and its handler:
    1. decide on a unique event name, e.g., 'RESCHEDULE'
    2. create an event entry point -- a source SimEntity that generates an event
       with this type, e.g., Event.params['type'] = 'RESCHEDULE'
    3. create an event handler on the destination SimEntity:
       def evtHandlerReschedule
    NOTE: We could have used __dict__ for mapping, but then the handler names
       would have been fixed to smth like evtEVENT_NAME_HERE, and they
       should have belonged to the destination SimEntity -- all these
       too restrictive to be useful.
    """

    events_map = {}

    def __init__(self, simulator, name):
        self.sim = simulator
        self.name = name

        self.id = self.sim.entity_registry.add_entity(self)

        self.events = self.sim.events
        self.config = self.sim.config

    def activate(self):
        """Generates first events for this entity."""

        pass

    def validate_event(self, event):
        """Can be overwritten by subclass to check for other attributes."""

        return False if event.params is None or \
                        'type' not in event.params or \
                        event.params['type'] not in self.events_map \
        else True

    def dispatch(self, event):
        if not self.validate_event(event):
            raise Exception('Failed to validate event {0}'.format(event))

        # call the event's handler, and pass to it the event's parameters
        event_type = event.params['type']
        self.events_map[event_type](event.params)


class EntityRegistry(object):
    def __init__(self):
        self._index = {}
        self.next_id = 0

    def __iter__(self):
        return iter(self._index.values())

    def add_entity(self, entity):
        id = self.next_id
        self.next_id += 1

        if id in self._index:
            raise KeyError('Registry already contains id {0}'.format(id))

        self._index[id] = entity

        return id

    def remove_entity_by_id(self, id):
        del self._index[id]

    def get_entity_by_id(self, id):
        if id >= self.next_id:
            raise KeyError('Id has not been assigned yet')

        return self._index.get(id, None)


class CSimulation(object):
    def __init__(self, event_queue=None, entity_registry=None, config=None):
        if not config:
            raise Exception('No config provided')

        self.config = config
        SimUtils.save_config(self.config)

        self.events = event_queue or EventQueue()
        self.entity_registry = entity_registry or EntityRegistry()

        # ts_now has to be initialized here, it could be used in self.setup()
        self.ts_now = 0

    def setup(self):
        """Overwrite for your own simulation."""

        pass

    def activate_entities(self):
        for entity in self.entity_registry:
            entity.activate()

    def dispatch(self, event):
        entity = self.entity_registry.get_entity_by_id(event.dest)

        if entity:
            entity.dispatch(event)
        else:
            # just for debugging purposes atm, it can be deleted in the future
            # it should signal that a site was dropped
            logger.debug('Can\'t deliver event {0}, entity {1} no longer regirested'.format(event, event.dest), extra={'ts_now': self.ts_now})

    def start(self, ts_end):
        """Overwrite for your own simulation."""

        self.ts_end = ts_end

        self.activate_entities()

        # start processing events
        while self.ts_now <= self.ts_end and self.events:
            event = self.events.dequeue()

            self.ts_now = event.ts_arrival
            if self.ts_now > self.ts_end:
                print 'Got an event with ts_arrival={0} > ts_end={1} --> ending simulation'.format(self.ts_now, self.ts_end)
                break

            self.dispatch(event)

    def report(self):
        """Overwrite for your own simulation."""

        pass


if __name__ == "__main__":
    arguments = docopt(__doc__)

    config = SimUtils.generate_config(
        N_TICKS=arguments['<N_TICKS>'],
        config_schema=config_schema
    )

    sim = CSimulation(config=config)
    sim.start(config['simulation']['N_TICKS'])
