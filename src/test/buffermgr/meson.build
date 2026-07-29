# Copyright (c) 2022-2025, PostgreSQL Global Development Group

test_install_data += files(
  'buffermgr_test.control',
  'buffermgr_test--1.0.sql',
)

tests += {
  'name': 'buffermgr',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'buffer_resize',
    ],
    'regress_args': ['--temp-config', files('buffermgr_test.conf')],
  },
  'tap': {
    'env': {
      'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_resize_fault_tolerance.pl',
      't/002_client_join_buffer_resize.pl',
      't/003_resize_failures.pl',
      't/004_resize_with_syslogger.pl',
      't/009_resize_checkpoint_stress.pl',
      't/010_stress_resize_buffer.pl',
      't/011_stress_drop_relation_buffers.pl',
      't/012_stress_drop_database_buffers.pl',
    ],
  },
}
