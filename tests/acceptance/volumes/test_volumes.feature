Feature: Habibi server volumes
    Habibi host machines should be able to share
    directories with habibi servers (docker containers)

    Scenario: share directory with server
        When i create farm and add role to it
        and start server with shared directory
        then i am able to see file created from inside
