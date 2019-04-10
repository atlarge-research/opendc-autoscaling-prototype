# from autoscalers.adapt_autoscaler import AdaptAutoscaler
# from autoscalers.conpaas_autoscaler import ConpaasAutoscaler
# from autoscalers.hist_autoscaler import HistAutoscaler
# from autoscalers.plan_autoscaler import PlanAutoscaler
# from autoscalers.react_autoscaler import ReactAutoscaler
# from autoscalers.reg_autoscaler import RegAutoscaler
# from autoscalers.token_autoscaler import TokenAutoscaler
# from autoscalers.token_mod_autoscaler import TokenModAutoscaler


def get_autoscaler_by_name(name):
    """
    Returns the correct autoscaler by name.
    Requires any new autoscaler class is added to the map below to be usable.
    :param name: the name of the autoscaler
    :return: the autoscaler class associated with the name or None
    """
    map = {
        # 'adapt': AdaptAutoscaler,
        # 'conpaas': ConpaasAutoscaler,
        # 'hist': HistAutoscaler,
        # 'plan': PlanAutoscaler,
        # 'react': ReactAutoscaler,
        # 'reg': RegAutoscaler,
        # 'token': TokenAutoscaler,
        # 'token_mod': TokenModAutoscaler,
    }

    return map[name] if name in map else None
