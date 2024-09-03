# Copyright (c) 20244-2024, Ashutosh Bapat

exprs_sources = files(
  'exprs.c',
)

exprs = shared_module('exprs',
  exprs_sources,
  kwargs: contrib_mod_args,
)

contrib_targets += exprs

install_data(
  'exprs--1.1.sql',
  'exprs.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'exprs',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'exprs',
    ],
  },
}
