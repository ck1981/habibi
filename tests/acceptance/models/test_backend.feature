Feature: Habibi entities saved to persistent DB
    Habibi stores it's entities in persistent DB using peewee ORM.

    Scenario: Create infrastructure
        Given I created habibi api object
        When I created new farm named 'spike-prod'
         And I created new role named 'base-ubuntu1204'
            """
            {"image": "ubuntu:12.04", "behaviors": ["base", "chef"]}
            """
         And I added this role to my farm
         And I created 2 servers of that new farm_role in zone 'us-east-1a'
            """
            {"ids": ["9bab5389-820d-4fea-b13a-539dc69bcf4a", "d32741f4-bb05-442f-abb4-c08ad972e242"]}
            """
         And I created new event 'CustomEvent' triggered by one of my servers
         And I set GVs
            | gv_name | gv_value | scope  | scope_id                                    |
            | Test    | value1   | farm   | 1                                           |
            | Test2   | value2   | role   | 1                                           |
            | Test2   | value3   | server | 9bab5389-820d-4fea-b13a-539dc69bcf4a        |
            | Test4   | value4   | server | d32741f4-bb05-442f-abb4-c08ad972e242        |

        When I try to find my scalr objects through API
        Then I receive exactly what I added before

    Scenario: Remove infrastructure

        # When I remove farm_role from my farm




