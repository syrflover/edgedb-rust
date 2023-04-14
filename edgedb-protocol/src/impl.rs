use bytes::BufMut;
use edgedb_errors::{DescriptorMismatch, Error, ErrorKind};

use crate::{
    descriptors::Descriptor,
    query_arg::{Encoder, QueryArg, QueryArgs},
};

impl<T> QueryArgs for Vec<T>
where
    T: QueryArg,
{
    fn encode(&self, enc: &mut Encoder) -> Result<(), Error> {
        let count = self.len();

        let root_pos = enc.ctx.root_pos.ok_or_else(|| {
            DescriptorMismatch::with_message(format!(
                "provided {} positional arguments, \
                 but no arguments expected by the server",
                count
            ))
        })?;
        let desc = enc.ctx.get(root_pos)?;

        match desc {
            Descriptor::ObjectShape(desc) if enc.ctx.proto.is_at_least(0, 12) => {
                if desc.elements.len() != count {
                    return Err(enc.ctx.field_number(desc.elements.len(), count));
                }
                let mut els = desc.elements.iter().enumerate();

                for x in self.iter() {
                    let (idx, el) = els.next().unwrap();
                    if el.name.parse() != Ok(idx) {
                        return Err(DescriptorMismatch::with_message(format!(
                            "expected positional arguments, \
                                     got {} instead of {}",
                            el.name, idx
                        )));
                    }
                    x.check_descriptor(enc.ctx, el.type_pos)?;
                }
            }
            Descriptor::Tuple(desc) if enc.ctx.proto.is_at_most(0, 11) => {
                if desc.element_types.len() != count {
                    return Err(enc.ctx.field_number(desc.element_types.len(), count));
                }
                let mut els = desc.element_types.iter();

                for x in self.iter() {
                    let type_pos = els.next().unwrap();
                    x.check_descriptor(enc.ctx, *type_pos)?;
                }
            }
            _ => {
                return Err(enc.ctx.wrong_type(
                    desc,
                    if enc.ctx.proto.is_at_least(0, 12) {
                        "object"
                    } else {
                        "tuple"
                    },
                ))
            }
        }

        enc.buf.reserve(4 + 8 * count);
        enc.buf.put_u32(count as u32);

        for x in self.iter() {
            enc.buf.reserve(8);
            enc.buf.put_u32(0);
            QueryArg::encode_slot(x, enc)?;
        }

        Ok(())
    }
}
