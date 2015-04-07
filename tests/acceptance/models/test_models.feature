Feature: Habibi mongodb persistent backend
    Habibi should be distributed, so all objects must be stored in persistent
    backend, like mongodb.

    Scenario: add servers from different connections
        Given I created farm with 1 role
        When I add 2 servers
        Then I can see that farm was updated

        When I reconnect
        Then I see that farm i added before, with same servers
