require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/edn_lines"
require "logstash/event"
require "logstash/json"
require "insist"
require "edn"

describe LogStash::Codecs::EDNLines do

  subject { LogStash::Codecs::EDNLines.new(config) }

  let(:config) { Hash.new }

  let(:data) { { "foo" => "bar", "baz" => {"bah" => ["a", "b", "c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z" } }

  context "#decode" do
    it "should return an event from edn data" do
      event_count = 0
      subject.decode(data.to_edn + "\n") do |event|
        event_count += 1
        insist { event }.is_a?(LogStash::Event)
        insist { event.get("foo") } == data["foo"]
        insist { event.get("baz") } == data["baz"]
        insist { event.get("bah") } == data["bah"]
        insist { event.get("@timestamp").to_iso8601 } == data["@timestamp"]
      end
      expect(event_count).to eql 1
    end

    it "should return an event from edn data when a newline is received" do
      subject.decode(data.to_edn) do |event|
        insist {false}
      end
      event_count = 0
      subject.decode("\n") do |event|
        event_count += 1
        insist { event.is_a? LogStash::Event }
        insist { event.get("foo") } == data["foo"]
        insist { event.get("baz") } == data["baz"]
        insist { event.get("bah") } == data["bah"]
        insist { event.get("@timestamp").to_iso8601 } == data["@timestamp"]
      end
      expect(event_count).to eql 1
    end

    context 'with target' do

      let(:config) { super().merge('target' => '[doc]') }

      it "decodes an event" do
        event_count = 0
        subject.decode(data.to_edn + "\n") do |event|
          event_count += 1
          expect(event.include?("foo")).to be false
          expect(event.get("[doc][foo]")).to eql 'bar'
          expect(event.get("[doc][baz]")).to eql "bah" => ["a","b","c"]
          expect(event.get("[doc][@timestamp]")).to eql data["@timestamp"]
        end
        expect(event_count).to eql 1
      end

    end
  end

  context "#encode" do
    it "should return edn data from pure ruby hash" do
      data = {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"}
      event = LogStash::Event.new(data)
      got_event = false
      subject.on_event do |e, d|
        insist { EDN.read(d)["foo"] } == data["foo"]
        insist { EDN.read(d)["baz"] } == data["baz"]
        insist { EDN.read(d)["bah"] } == data["bah"]
        insist { EDN.read(d)["@timestamp"] } == "2014-05-30T02:52:17.929Z"
        insist { EDN.read(d)["@timestamp"] } == event.get("@timestamp").to_iso8601
       got_event = true
      end
      subject.encode(event)
      expect(got_event).to be true
    end

    it "should return edn data rom deserialized json with normalization" do
      data = LogStash::Json.load('{"foo": "bar", "baz": {"bah": ["a","b","c"]}, "@timestamp": "2014-05-30T02:52:17.929Z"}')
      event = LogStash::Event.new(data)
      got_event = false
      subject.on_event do |e, d|
        insist { EDN.read(d)["foo"] } == data["foo"]
        insist { EDN.read(d)["baz"] } == data["baz"]
        insist { EDN.read(d)["bah"] } == data["bah"]
        insist { EDN.read(d)["@timestamp"] } == "2014-05-30T02:52:17.929Z"
        insist { EDN.read(d)["@timestamp"] } == event.get("@timestamp").to_iso8601
       got_event = true
      end
      subject.encode(event)
      expect(got_event).to be true
    end
  end

end
