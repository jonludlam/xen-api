(*
 * Copyright (C) Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *)


let to_API_data_source (ds : Rrd_idl.t) = {
  API.data_source_name_label = ds.Rrd_idl.name;
  data_source_name_description = ds.Rrd_idl.description;
  data_source_enabled = ds.Rrd_idl.enabled;
  data_source_standard = ds.Rrd_idl.standard;
  data_source_units = ds.Rrd_idl.units;
  data_source_min = ds.Rrd_idl.min;
  data_source_max = ds.Rrd_idl.max;
  data_source_value = 0.;
}
