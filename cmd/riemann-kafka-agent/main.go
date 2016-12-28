package main

import (
  "flag"
  "github.com/wvanbergen/kazoo-go"
  "github.com/Shopify/sarama"
  "log"
  "strconv"
  "time"
)

func serviceName(suffix string, topic string, consumerGroupName string, partitionId int32, service string) string {
  return suffix + "." + topic + "." + consumerGroupName + "." + strconv.Itoa(int(partitionId)) + "." + service
}

func main() {
  var (
    zkAddrs     = flag.String("zookeeper-nodes", "", "Zookeeper nodes (e.g. host1:2181,host2:2181)")

    riemannHost = flag.String("riemann-host", "localhost", "Riemann host")
    riemannPort = flag.Int("riemann-port", 5555, "Riemann port")
    riemannReconnectInterval = flag.Int("riemann-reconnect-interval", 5, "Riemann reconnect interval")

    sendInterval  = flag.Int("send-interval", 5, "Metric send interval")
  )
  flag.Parse()

  ticker := time.NewTicker(time.Second * time.Duration(*sendInterval))

  riemann := NewRiemann(*riemannHost, *riemannPort, *riemannReconnectInterval)
  go riemann.Run()

  var zookeeperNodes []string
  zookeeperNodes, chroot := kazoo.ParseConnectionString(*zkAddrs)

  log.Printf("zookeeperNodes: %v", zookeeperNodes)

  var kz *kazoo.Kazoo
  conf := kazoo.NewConfig()
  conf.Chroot = chroot
  kz, err := kazoo.NewKazoo(zookeeperNodes, conf)
  if err != nil {
    panic(err)
  }
  defer kz.Close()

  brokers, err := kz.BrokerList()
  if err != nil {
    panic(err)
  }
  log.Printf("KZ Brokers: %v", brokers)

  consumerGroupList, err := kz.Consumergroups()
  if err != nil {
    panic(err)
  }

  client, err := sarama.NewClient(brokers, nil)
  if err != nil {
    panic(err)
  }

  for _ = range ticker.C {
    for _, consumerGroup := range consumerGroupList {
      offsets, err := consumerGroup.FetchAllOffsets()
      if err != nil {
        panic(err)
      }

      for topic, consumerGroupPartitionOffsets := range offsets {
        for partitionId, offset := range consumerGroupPartitionOffsets {
          partitionSize, err := client.GetOffset(topic, partitionId, sarama.OffsetNewest)
          if err != nil {
            panic(err)
          }
          lag := partitionSize - offset

          riemann.InputChan <- RiemannEvent{
            Service:  serviceName("kafka", topic, consumerGroup.Name, partitionId, "lag"),
            Metric:   lag,
          }

          riemann.InputChan <- RiemannEvent{
            Service:  serviceName("kafka", topic, consumerGroup.Name, partitionId, "offset"),
            Metric:   offset,
          }

          riemann.InputChan <- RiemannEvent{
            Service:  serviceName("kafka", topic, consumerGroup.Name, partitionId, "partition_size"),
            Metric:   partitionSize,
          }
        }
      }
    }
  }
}
