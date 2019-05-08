// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: k8s.io/apimachinery/pkg/apis/meta/v1beta1/generated.proto

/*
	Package v1beta1 is a generated protocol buffer package.

	It is generated from these files:
		k8s.io/apimachinery/pkg/apis/meta/v1beta1/generated.proto

	It has these top-level messages:
		PartialObjectMetadata
		PartialObjectMetadataList
		TableOptions
*/
package v1beta1

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import k8s_io_apimachinery_pkg_apis_meta_v1 "github.com/ericchiang/k8s/apis/meta/v1"
import _ "github.com/ericchiang/k8s/runtime"
import _ "github.com/ericchiang/k8s/runtime/schema"
import _ "github.com/ericchiang/k8s/util/intstr"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// PartialObjectMetadata is a generic representation of any object with ObjectMeta. It allows clients
// to get access to a particular ObjectMeta schema without knowing the details of the version.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PartialObjectMetadata struct {
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	Metadata         *k8s_io_apimachinery_pkg_apis_meta_v1.ObjectMeta `protobuf:"bytes,1,opt,name=metadata" json:"metadata,omitempty"`
	XXX_unrecognized []byte                                           `json:"-"`
}

func (m *PartialObjectMetadata) Reset()                    { *m = PartialObjectMetadata{} }
func (m *PartialObjectMetadata) String() string            { return proto.CompactTextString(m) }
func (*PartialObjectMetadata) ProtoMessage()               {}
func (*PartialObjectMetadata) Descriptor() ([]byte, []int) { return fileDescriptorGenerated, []int{0} }

func (m *PartialObjectMetadata) GetMetadata() *k8s_io_apimachinery_pkg_apis_meta_v1.ObjectMeta {
	if m != nil {
		return m.Metadata
	}
	return nil
}

// PartialObjectMetadataList contains a list of objects containing only their metadata
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PartialObjectMetadataList struct {
	// items contains each of the included items.
	Items            []*PartialObjectMetadata `protobuf:"bytes,1,rep,name=items" json:"items,omitempty"`
	XXX_unrecognized []byte                   `json:"-"`
}

func (m *PartialObjectMetadataList) Reset()         { *m = PartialObjectMetadataList{} }
func (m *PartialObjectMetadataList) String() string { return proto.CompactTextString(m) }
func (*PartialObjectMetadataList) ProtoMessage()    {}
func (*PartialObjectMetadataList) Descriptor() ([]byte, []int) {
	return fileDescriptorGenerated, []int{1}
}

func (m *PartialObjectMetadataList) GetItems() []*PartialObjectMetadata {
	if m != nil {
		return m.Items
	}
	return nil
}

// TableOptions are used when a Table is requested by the caller.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TableOptions struct {
	// includeObject decides whether to include each object along with its columnar information.
	// Specifying "None" will return no object, specifying "Object" will return the full object contents, and
	// specifying "Metadata" (the default) will return the object's metadata in the PartialObjectMetadata kind
	// in version v1beta1 of the meta.k8s.io API group.
	IncludeObject    *string `protobuf:"bytes,1,opt,name=includeObject" json:"includeObject,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *TableOptions) Reset()                    { *m = TableOptions{} }
func (m *TableOptions) String() string            { return proto.CompactTextString(m) }
func (*TableOptions) ProtoMessage()               {}
func (*TableOptions) Descriptor() ([]byte, []int) { return fileDescriptorGenerated, []int{2} }

func (m *TableOptions) GetIncludeObject() string {
	if m != nil && m.IncludeObject != nil {
		return *m.IncludeObject
	}
	return ""
}

func init() {
	proto.RegisterType((*PartialObjectMetadata)(nil), "k8s.io.apimachinery.pkg.apis.meta.v1beta1.PartialObjectMetadata")
	proto.RegisterType((*PartialObjectMetadataList)(nil), "k8s.io.apimachinery.pkg.apis.meta.v1beta1.PartialObjectMetadataList")
	proto.RegisterType((*TableOptions)(nil), "k8s.io.apimachinery.pkg.apis.meta.v1beta1.TableOptions")
}
func (m *PartialObjectMetadata) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PartialObjectMetadata) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Metadata != nil {
		dAtA[i] = 0xa
		i++
		i = encodeVarintGenerated(dAtA, i, uint64(m.Metadata.Size()))
		n1, err := m.Metadata.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *PartialObjectMetadataList) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PartialObjectMetadataList) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Items) > 0 {
		for _, msg := range m.Items {
			dAtA[i] = 0xa
			i++
			i = encodeVarintGenerated(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *TableOptions) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TableOptions) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.IncludeObject != nil {
		dAtA[i] = 0xa
		i++
		i = encodeVarintGenerated(dAtA, i, uint64(len(*m.IncludeObject)))
		i += copy(dAtA[i:], *m.IncludeObject)
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeVarintGenerated(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *PartialObjectMetadata) Size() (n int) {
	var l int
	_ = l
	if m.Metadata != nil {
		l = m.Metadata.Size()
		n += 1 + l + sovGenerated(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *PartialObjectMetadataList) Size() (n int) {
	var l int
	_ = l
	if len(m.Items) > 0 {
		for _, e := range m.Items {
			l = e.Size()
			n += 1 + l + sovGenerated(uint64(l))
		}
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *TableOptions) Size() (n int) {
	var l int
	_ = l
	if m.IncludeObject != nil {
		l = len(*m.IncludeObject)
		n += 1 + l + sovGenerated(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovGenerated(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozGenerated(x uint64) (n int) {
	return sovGenerated(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *PartialObjectMetadata) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowGenerated
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PartialObjectMetadata: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PartialObjectMetadata: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Metadata", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthGenerated
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Metadata == nil {
				m.Metadata = &k8s_io_apimachinery_pkg_apis_meta_v1.ObjectMeta{}
			}
			if err := m.Metadata.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipGenerated(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthGenerated
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PartialObjectMetadataList) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowGenerated
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PartialObjectMetadataList: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PartialObjectMetadataList: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Items", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthGenerated
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Items = append(m.Items, &PartialObjectMetadata{})
			if err := m.Items[len(m.Items)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipGenerated(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthGenerated
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *TableOptions) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowGenerated
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TableOptions: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TableOptions: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field IncludeObject", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthGenerated
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			s := string(dAtA[iNdEx:postIndex])
			m.IncludeObject = &s
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipGenerated(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthGenerated
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipGenerated(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowGenerated
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthGenerated
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowGenerated
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipGenerated(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthGenerated = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowGenerated   = fmt.Errorf("proto: integer overflow")
)

func init() {
	proto.RegisterFile("k8s.io/apimachinery/pkg/apis/meta/v1beta1/generated.proto", fileDescriptorGenerated)
}

var fileDescriptorGenerated = []byte{
	// 289 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0xd1, 0xcd, 0x4a, 0xc3, 0x40,
	0x14, 0x05, 0x60, 0x07, 0x11, 0x75, 0xaa, 0x9b, 0x80, 0xd0, 0x76, 0x11, 0x4a, 0x71, 0x51, 0x37,
	0x33, 0xb6, 0x16, 0xd1, 0x9d, 0xb8, 0xae, 0x54, 0x8a, 0xb8, 0x70, 0x77, 0x9b, 0x5c, 0xd2, 0x31,
	0x99, 0x49, 0x98, 0xb9, 0x11, 0x7c, 0x13, 0x1f, 0xc9, 0xa5, 0x8f, 0x20, 0xf1, 0x45, 0x24, 0x26,
	0xf8, 0xd3, 0x1f, 0xcc, 0x2e, 0x1c, 0xf2, 0x9d, 0xc3, 0xcc, 0xf0, 0xcb, 0xf8, 0xc2, 0x09, 0x95,
	0x4a, 0xc8, 0x94, 0x86, 0x60, 0xa1, 0x0c, 0xda, 0x67, 0x99, 0xc5, 0x51, 0x19, 0x38, 0xa9, 0x91,
	0x40, 0x3e, 0x0d, 0xe7, 0x48, 0x30, 0x94, 0x11, 0x1a, 0xb4, 0x40, 0x18, 0x8a, 0xcc, 0xa6, 0x94,
	0x7a, 0x27, 0x15, 0x15, 0xbf, 0xa9, 0xc8, 0xe2, 0xa8, 0x0c, 0x9c, 0x28, 0xa9, 0xa8, 0x69, 0x77,
	0xdc, 0x64, 0x65, 0x79, 0xa0, 0x2b, 0x37, 0x29, 0x9b, 0x1b, 0x52, 0x1a, 0x57, 0xc0, 0xf9, 0x7f,
	0xc0, 0x05, 0x0b, 0xd4, 0xb0, 0xe2, 0xce, 0x36, 0xb9, 0x9c, 0x54, 0x22, 0x95, 0x21, 0x47, 0x76,
	0x19, 0xf5, 0x91, 0x1f, 0xdd, 0x82, 0x25, 0x05, 0xc9, 0x74, 0xfe, 0x88, 0x01, 0xdd, 0x20, 0x41,
	0x08, 0x04, 0xde, 0x84, 0xef, 0xe9, 0xfa, 0xbb, 0xcd, 0x7a, 0x6c, 0xd0, 0x1a, 0x9d, 0x8a, 0x26,
	0x57, 0x25, 0x7e, 0x7a, 0x66, 0xdf, 0x0d, 0x7d, 0xc7, 0x3b, 0x6b, 0x67, 0x26, 0xca, 0x91, 0x77,
	0xcf, 0x77, 0x14, 0xa1, 0x76, 0x6d, 0xd6, 0xdb, 0x1e, 0xb4, 0x46, 0x57, 0xa2, 0xf1, 0x93, 0x88,
	0xb5, 0xa5, 0xb3, 0xaa, 0xae, 0x3f, 0xe6, 0x07, 0x77, 0x30, 0x4f, 0x70, 0x9a, 0x91, 0x4a, 0x8d,
	0xf3, 0x8e, 0xf9, 0xa1, 0x32, 0x41, 0x92, 0x87, 0x58, 0xfd, 0xff, 0x75, 0xae, 0xfd, 0xd9, 0xdf,
	0xf0, 0xba, 0xf3, 0x5a, 0xf8, 0xec, 0xad, 0xf0, 0xd9, 0x7b, 0xe1, 0xb3, 0x97, 0x0f, 0x7f, 0xeb,
	0x61, 0xb7, 0x5e, 0xfb, 0x0c, 0x00, 0x00, 0xff, 0xff, 0x0a, 0x10, 0x6f, 0xa8, 0x67, 0x02, 0x00,
	0x00,
}
