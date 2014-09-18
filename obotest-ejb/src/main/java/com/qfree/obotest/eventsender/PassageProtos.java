// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: protobuf_test1.proto

package com.qfree.obotest.eventsender;

public final class PassageProtos {
  private PassageProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface PassageOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional string image_name = 1;
    /**
     * <code>optional string image_name = 1;</code>
     */
    boolean hasImageName();
    /**
     * <code>optional string image_name = 1;</code>
     */
    java.lang.String getImageName();
    /**
     * <code>optional string image_name = 1;</code>
     */
    com.google.protobuf.ByteString
        getImageNameBytes();

    // optional bytes image = 2;
    /**
     * <code>optional bytes image = 2;</code>
     */
    boolean hasImage();
    /**
     * <code>optional bytes image = 2;</code>
     */
    com.google.protobuf.ByteString getImage();
  }
  /**
   * Protobuf type {@code protobuf_test1.Passage}
   */
  public static final class Passage extends
      com.google.protobuf.GeneratedMessage
      implements PassageOrBuilder {
    // Use Passage.newBuilder() to construct.
    private Passage(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Passage(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Passage defaultInstance;
    public static Passage getDefaultInstance() {
      return defaultInstance;
    }

    public Passage getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Passage(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              imageName_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              image_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
			return com.qfree.obotest.eventsender.PassageProtos.internal_static_protobuf_test1_Passage_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
			return com.qfree.obotest.eventsender.PassageProtos.internal_static_protobuf_test1_Passage_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
							com.qfree.obotest.eventsender.PassageProtos.Passage.class,
							com.qfree.obotest.eventsender.PassageProtos.Passage.Builder.class);
    }

    public static com.google.protobuf.Parser<Passage> PARSER =
        new com.google.protobuf.AbstractParser<Passage>() {
      public Passage parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Passage(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<Passage> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional string image_name = 1;
    public static final int IMAGE_NAME_FIELD_NUMBER = 1;
    private java.lang.Object imageName_;
    /**
     * <code>optional string image_name = 1;</code>
     */
    public boolean hasImageName() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string image_name = 1;</code>
     */
    public java.lang.String getImageName() {
      java.lang.Object ref = imageName_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          imageName_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string image_name = 1;</code>
     */
    public com.google.protobuf.ByteString
        getImageNameBytes() {
      java.lang.Object ref = imageName_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        imageName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional bytes image = 2;
    public static final int IMAGE_FIELD_NUMBER = 2;
    private com.google.protobuf.ByteString image_;
    /**
     * <code>optional bytes image = 2;</code>
     */
    public boolean hasImage() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional bytes image = 2;</code>
     */
    public com.google.protobuf.ByteString getImage() {
      return image_;
    }

    private void initFields() {
      imageName_ = "";
      image_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getImageNameBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, image_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getImageNameBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, image_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }

		public static com.qfree.obotest.eventsender.PassageProtos.Passage parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }

		public static Builder newBuilder(com.qfree.obotest.eventsender.PassageProtos.Passage prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code protobuf_test1.Passage}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
				implements com.qfree.obotest.eventsender.PassageProtos.PassageOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
				return com.qfree.obotest.eventsender.PassageProtos.internal_static_protobuf_test1_Passage_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
				return com.qfree.obotest.eventsender.PassageProtos.internal_static_protobuf_test1_Passage_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
								com.qfree.obotest.eventsender.PassageProtos.Passage.class,
								com.qfree.obotest.eventsender.PassageProtos.Passage.Builder.class);
      }

			// Construct using com.qfree.obotest.eventsender.PassageProtos.Passage.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        imageName_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        image_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
				return com.qfree.obotest.eventsender.PassageProtos.internal_static_protobuf_test1_Passage_descriptor;
      }

			public com.qfree.obotest.eventsender.PassageProtos.Passage getDefaultInstanceForType() {
				return com.qfree.obotest.eventsender.PassageProtos.Passage.getDefaultInstance();
      }

			public com.qfree.obotest.eventsender.PassageProtos.Passage build() {
				com.qfree.obotest.eventsender.PassageProtos.Passage result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

			public com.qfree.obotest.eventsender.PassageProtos.Passage buildPartial() {
				com.qfree.obotest.eventsender.PassageProtos.Passage result = new com.qfree.obotest.eventsender.PassageProtos.Passage(
						this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.imageName_ = imageName_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.image_ = image_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
				if (other instanceof com.qfree.obotest.eventsender.PassageProtos.Passage) {
					return mergeFrom((com.qfree.obotest.eventsender.PassageProtos.Passage) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

			public Builder mergeFrom(com.qfree.obotest.eventsender.PassageProtos.Passage other) {
				if (other == com.qfree.obotest.eventsender.PassageProtos.Passage.getDefaultInstance())
					return this;
        if (other.hasImageName()) {
          bitField0_ |= 0x00000001;
          imageName_ = other.imageName_;
          onChanged();
        }
        if (other.hasImage()) {
          setImage(other.getImage());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
				com.qfree.obotest.eventsender.PassageProtos.Passage parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
					parsedMessage = (com.qfree.obotest.eventsender.PassageProtos.Passage) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional string image_name = 1;
      private java.lang.Object imageName_ = "";
      /**
       * <code>optional string image_name = 1;</code>
       */
      public boolean hasImageName() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional string image_name = 1;</code>
       */
      public java.lang.String getImageName() {
        java.lang.Object ref = imageName_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          imageName_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string image_name = 1;</code>
       */
      public com.google.protobuf.ByteString
          getImageNameBytes() {
        java.lang.Object ref = imageName_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          imageName_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string image_name = 1;</code>
       */
      public Builder setImageName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        imageName_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string image_name = 1;</code>
       */
      public Builder clearImageName() {
        bitField0_ = (bitField0_ & ~0x00000001);
        imageName_ = getDefaultInstance().getImageName();
        onChanged();
        return this;
      }
      /**
       * <code>optional string image_name = 1;</code>
       */
      public Builder setImageNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        imageName_ = value;
        onChanged();
        return this;
      }

      // optional bytes image = 2;
      private com.google.protobuf.ByteString image_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes image = 2;</code>
       */
      public boolean hasImage() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional bytes image = 2;</code>
       */
      public com.google.protobuf.ByteString getImage() {
        return image_;
      }
      /**
       * <code>optional bytes image = 2;</code>
       */
      public Builder setImage(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        image_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes image = 2;</code>
       */
      public Builder clearImage() {
        bitField0_ = (bitField0_ & ~0x00000002);
        image_ = getDefaultInstance().getImage();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:protobuf_test1.Passage)
    }

    static {
      defaultInstance = new Passage(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:protobuf_test1.Passage)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_protobuf_test1_Passage_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_protobuf_test1_Passage_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\024protobuf_test1.proto\022\016protobuf_test1\"," +
      "\n\007Passage\022\022\n\nimage_name\030\001 \001(\t\022\r\n\005image\030\002" +
      " \001(\014B;\n*com.qfree.jeffreyz.rabbitmq.prot" +
      "obuf_test1B\rPassageProtos"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_protobuf_test1_Passage_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_protobuf_test1_Passage_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_protobuf_test1_Passage_descriptor,
              new java.lang.String[] { "ImageName", "Image", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
