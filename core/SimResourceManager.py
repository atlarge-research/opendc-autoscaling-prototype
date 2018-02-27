from core import Constants
from utils import SimUtils


class ResourceManager(object):
    def __init__(self, logger, simulator, SiteClass, cluster_setup, allow_duplicates=False):
        self.logger = logger
        self.simulator = simulator
        self.SiteClass = SiteClass

        self.allow_duplicates = allow_duplicates # for a neverending supply of available sites
        self.next_site_id = 0 # used only for duplicates

        if not cluster_setup:
            raise Exception('No clusters found')
        self.cluster_setup = sorted(cluster_setup, key=lambda cluster: cluster.NProcs)

        self.sites = []
        self.start_all_available_sites()

    def get_current_capacity(self):
        current_capacity = 0
        for site in self.sites:
            if site.status == Constants.STATUS_RUNNING:
                current_capacity += site.resources

        return current_capacity

    def get_maximum_capacity(self):
        if self.allow_duplicates:
            raise NotImplementedError

        max_capacity = 0
        for site in self.sites:
            max_capacity += site.resources

        max_capacity += sum(site.NProcs for site in self.get_available_sites())

        return max_capacity

    def get_available_sites(self):
        '''Sites from cluster_setup that can be started.'''

        if self.allow_duplicates:
            return self.cluster_setup

        running_sites_IDs = [site.name for site in self.sites]
        return [site for site in self.cluster_setup if site.ClusterID not in running_sites_IDs]

    def start_all_available_sites(self):
        available_sites = self.get_available_sites()

        self.logger.log_and_db('Starting {0} sites'.format(len(available_sites)))

        resources = 0
        for site_info in available_sites:
            resources += self._provision_site(site_info)

        return resources

    def start_smallest_available_site(self, min_size=None):
        available_sites = self.get_available_sites()
        if min_size:
            try:
                # extract the first smallest site that can fullfill the resource requirements
                available_sites = [next(site for site in available_sites if site.NProcs >= min_size)]
            except:
                available_sites = []

        if not available_sites:
            return 0

        self.logger.log('Starting smallest site available')
        return self._provision_site(available_sites[0])

    def start_up_best_effort(self, capacity, fix_capacity=False):
        """
        If fix_capacity is True, it starts up sites only if it can attain said capacity without
        over or under provisioning.
        """

        # If we do not allow duplicates and have allocated all sites, return 0
        if not self.allow_duplicates and len(self.sites) == len(self.cluster_setup):
            return 0

        available_sites = self.get_available_sites()

        key = lambda site_info: site_info.NProcs
        sites_to_start = SimUtils.subset_closest_to_sum(available_sites, capacity, key, self.allow_duplicates)

        resources = 0
        if not fix_capacity or sum(map(key, sites_to_start)) == capacity:
            for site_info in sites_to_start:
                resources += self._provision_site(site_info)

        return resources

    def _provision_site(self, site_info):
        # generate unique names in case we allow duplicates
        site_name = '{0}{1}'.format(site_info.ClusterID, '_' + str(self.next_site_id) if self.allow_duplicates else '')
        self.next_site_id += 1

        new_site = self.SiteClass(
            simulator=self.simulator,
            name=site_name,
            resources=site_info.NProcs,
            resource_speed=1,  # TODO Laurens: set the speed from the file here instead of hardcoded 1?
            # ResourceSpeed=cluster.resource_speed,
        )

        self.sites.append(new_site)
        self.simulator.central_queue.add_site_stats(new_site)

        self.logger.log_and_db('Starting site {0} with {1} NProcs'.format(site_name, site_info.NProcs))

        return new_site.resources

    def stop_smallest_available_site(self, min_size=None, force=False):
        found_smallest = None

        for site in self.sites:
            if min_size and site.resources < min_size:
                continue

            # between two sites with equal resources, choose the idle one
            if (site.is_idle() and force and found_smallest and
                    found_smallest.resources == site.resources and not found_smallest.is_idle):
                found_smallest = site
            elif site.is_idle() or force and (not found_smallest or found_smallest.resources > site.resources):
                found_smallest = site

        return self.stop_site(self.sites.index(found_smallest)) if found_smallest else 0

    def release_resources_best_effort(self, capacity, only_idle=True, fix_capacity=False):
        """
        :param only_idle: if True, only releases idle VMs.
        :param fix_capacity: if True, performs release if it finds a combination of VMs whose resources sum up to target capacity.

        If both only_idle and fix_capacity flags are set, releases a set of idle VMs whose resource sum up to target capacity.
        """

        find_by_capacity = lambda site: site.resources
        select_by_idle = lambda site: float(site.used_resources) / site.resources

        # canditate sites considered for resource release
        sites_running = [site for site in self.sites if site.status == Constants.STATUS_RUNNING]

        if only_idle:
            # filter out sites that are not idle
            sites_running = [site for site in sites_running if site.is_idle()]
            sites_to_stop = SimUtils.subset_closest_to_sum(sites_running, capacity, key=find_by_capacity, gt=False)
        else:
            # finds best combination of sites by capacity first and idleness second
            sites_to_stop = SimUtils.subset_closest_to_sum2(sites_running, capacity,
                                                        key=find_by_capacity,
                                                        key2=select_by_idle)

        resources = 0
        if not fix_capacity or sum(map(find_by_capacity, sites_to_stop)) == capacity:
            for site in sites_to_stop:
                if only_idle and not site.is_idle():
                    if fix_capacity:  # won't be able to reach fix capacity
                        break
                    continue

                resources += site.resources
                site.shutdown()
                self.simulator.central_queue.remove_site_stats(site.id)

        return resources

    def stop_site(self, site_index):
        site = self.sites[site_index]
        self.logger.log('Stopping site {0}, id {1} with {2} free resources'.format(site.name, site.id, site.free_resources))

        resources = site.resources
        site.shutdown()
        self.simulator.central_queue.remove_site_stats(site.id)

        return resources

    def drop_site(self, site):
        self.logger.log('Dropping site {0}, id {1} with {2} free resources'.format(site.name, site.id, site.free_resources))
        if site.status != Constants.STATUS_SHUTDOWN:
            raise Exception('Only sites with shutdown status should be dropped')

        self.simulator.entity_registry.remove_entity_by_id(site.id)
        self.sites.remove(site)
