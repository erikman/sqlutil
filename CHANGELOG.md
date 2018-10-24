# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.1.0] - 2018-10-24
### Changed
- Fix bug when reading defaultValue from database for non integers.
- Bump dependencies

## [3.0.0] - 2018-07-03
### Added
- Bumped sqlite3 version to 4.0.1
- Bumped dependencies to fix vulnerabilities

## [2.2.0] - 2018-04-02
### Added
- Query class for interfacing with custom queries (like statements, but only for
  onetime use).
- Statement class with same API as query class.

### Deprecated
- SqlQuery class, renamed to BuildQuery

## [2.1.0] - 2018-03-16
### Added
- Collate option in schema for columns

### Changed
- Refactored into multiple files

## [2.0.1] - 2017-11-02
### Added
- Promise function for closing the database: `Database.close()`

## [2.0.0] - 2017-10-28
### Added
- Foreign keys support

### Changed
- createTable don't automatically update the schema, use createOrUpdateTable if
  you want the old behavior.

## [1.0.0] - 2017-06-26
- Initial release
