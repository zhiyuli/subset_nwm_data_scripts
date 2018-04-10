class SubsetJobElement:

    def __init__(self, region, region_type, simulation_date, file_type, model_cfg,
                 data_type, timestamp, nwm_version="v1.1", resize_dimension=True):

      self.simulation_date = simulation_date
      self.file_type = file_type  # forcing or forecast
      self.model_cfg = model_cfg # aa sr mr lr
      self.data_type = data_type # channel, reservoir, land , terrain
      self.timestamp = timestamp
      self.region = region
      self.region_type = region_type
      self.resize_dimension = resize_dimension
      self.nwm_version = nwm_version
