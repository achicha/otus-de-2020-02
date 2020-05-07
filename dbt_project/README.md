1. installation
        
        # install dbt
        pip install dbt
        
        # create connection profile for dbt 
        copy profiles.yml to `~/.dbt/profiles.yml`
        
        # run local postgres
        docker pull postgres
        docker run --name postgres -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 postgres
        
        # install pg cli
        brew uninstall --force postgresql
        brew install postgres
        
        # login:pass postgres:postgres
        psql -h localhost -p 5432 -U postgres

2. run project

        # clone repo
        git clone https://github.com/achicha/otus-de-2020-02.git
        
        # ensure profile setup
        dbt debug
        
        # load demo data
        dbt seed
        
        # run models
        dbt run
        
        # test models
        dbt test
        
        # Generate docs
        dbt docs generate
        
        # View docs
        dbt docs serve
