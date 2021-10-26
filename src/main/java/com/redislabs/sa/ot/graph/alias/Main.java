package com.redislabs.sa.ot.graph.alias;

import com.github.javafaker.Faker;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import com.redislabs.sa.ot.util.JedisConnectionFactory;
import redis.clients.jedis.JedisPool;

/**
 * This class loads RedisGraph with many graph nodes representing
 * Person ( a person with a name and an id )
 * Alias ( another name for that person )
 * Some Person nodes have two Aliases
 * Relationships are of the type: aka
 * (Also Known As)
 *
 * a query such as this: will return the Person matching the id and any Aliases they use
 * GRAPH.QUERY "ALIAS_GRAPH" "MATCH (p:Person {id: '184000'})--(x) return p, x"
 *
 * You can search for specific alias ranks:
 * GRAPH.QUERY "ALIAS_GRAPH" "MATCH (y)--(p:Person)--(x {rank: 4}) return y,p,x"
 *
 * To start the program and load the default of 20,000 Person nodes you can simply issue:
 * mvn compile exec:java
 *
 * You can also provide a value for the number of Pwrson nodes to create:
 * Note: it can take a while to populate a graph with 500K Person nodes (about 350MB in size) ~ 17 min
 * mvn compile exec:java -Dexec.args="500000"
 *
 */

public class Main {

    static JedisPool jPool = JedisConnectionFactory.getInstance().getJedisPool();
    static String GRAPH_NAME = "ALIAS_GRAPH";

    static RedisGraph graph = new RedisGraph(jPool);

    public static void main(String[] args){
        long graphBaseNodeSize=20000;
        try {
            graph.deleteGraph(GRAPH_NAME);
        }catch(redis.clients.jedis.exceptions.JedisDataException jde){System.out.println(GRAPH_NAME+" doesn't exist ... continuing on\n");}
        if(args.length>0) {
            graphBaseNodeSize = Long.parseLong(args[0]);
        }
        loadGraph(graphBaseNodeSize);
        createGraphIndexes();
    }

    static void createGraphIndexes() {
        String query = "create index ON :Person(id)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
        query = "create index ON :Alias(rank)";
        System.out.println(query);
        graph.query(GRAPH_NAME, query);
    }


    static void loadGraph(long numberOfBaseNodes) {
        Faker faker = new Faker();
        String name = faker.name().firstName()+" "+faker.book().author()+" "+faker.name().lastName();
        name = name.replaceAll("'","");
        String alias = faker.commerce().color() + faker.name().username();
        alias= alias.replaceAll("'","");
        String company = faker.company().name();
        company = company.replaceAll("'","");
        int rank = faker.finance().hashCode() % 100;
        String secondAlias = "";
        int secondRank = 0;
        String query = "CREATE (:Person { id: '0', " +
                "name: '" + name + "', creationDateTimeMillis: "+System.currentTimeMillis()+
                ", certified: 'true', company: '"+company+"'})" +
                "-[:aka]->(:Alias {moniker:'" + alias + "', rank:" + rank + "})";
        System.out.println("loadGraph() EXECUTING: \n" + query);
        for (long x = 1; x < numberOfBaseNodes; x++) {
            graph.query(GRAPH_NAME, query);
            name = faker.name().firstName()+" "+faker.book().author()+" "+faker.name().lastName();
            name = name.replaceAll("'","");
            alias = faker.commerce().color() + faker.name().username();
            company = faker.company().name();
            company = company.replaceAll("'","");
            alias= alias.replaceAll("'","");
            rank = faker.finance().hashCode() % 100;
            query = "CREATE (:Person { id: '" + x + "', " +
                    "name: '" + name + "', creationDateTimeMillis: "+System.currentTimeMillis()+
                    ", certified: 'true', company: '"+company+"'})" +
                    "-[:aka]->(:Alias {moniker:'" + alias + "', rank:" + rank + "})";
            if((x%500==0)&& (System.currentTimeMillis()%2==0)){
                secondAlias = faker.commerce().color() + faker.name().username();
                secondAlias = secondAlias.replaceAll("'","");
                secondRank = (faker.finance().hashCode() % 5)+1000;
                query = "CREATE (:Alias {moniker:'" + secondAlias + "', rank:" + secondRank + "})" +
                        "<-[:aka]-(:Person { id: '" + x + "', " +
                        "name: '" + name + "', creationDateTimeMillis: " +System.currentTimeMillis()+
                        ", certified: 'true', company: '"+company+"'})-[:aka]->(:Alias {moniker:'" + alias + "', rank:" + rank + "})";
                System.out.println("loadGraph() EXECUTING: \n" + query);
            }
        }
    }

}
